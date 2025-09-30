package main

import (
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/oracle/oci-go-sdk/v65/common"
	"github.com/oracle/oci-go-sdk/v65/objectstorage"
)

// Configuration
type Config struct {
	MaxWorkers int
	Days       int
	DownloadFolder string
	ReportFile string
}

// OperationResult tracks download results
type OperationResult struct {
	FileName    string
	FileSize    int64
	ReportDate  string
	Status      string
	Downloaded  bool
	Error       string
	LastAttempt time.Time
}

// Job represents a file to download
type Job struct {
	ObjectName string
	Namespace  string
	BucketName string
}

// Result represents the outcome of processing a job
type Result struct {
	Job    Job
	Result OperationResult
	Error  error
}

// Worker pool for concurrent downloads
type WorkerPool struct {
	jobs    chan Job
	results chan Result
	wg      sync.WaitGroup
	config  Config
	client  objectstorage.ObjectStorageClient
	ctx     context.Context
}

// parseDateFromName extracts date from object names
func parseDateFromName(name string) (time.Time, error) {
	parts := strings.Split(name, "/")
	if len(parts) < 4 {
		return time.Time{}, fmt.Errorf("invalid object name format: %s", name)
	}
	year, err := strconv.Atoi(parts[len(parts)-4])
	if err != nil {
		return time.Time{}, err
	}
	month, err := strconv.Atoi(parts[len(parts)-3])
	if err != nil {
		return time.Time{}, err
	}
	day, err := strconv.Atoi(parts[len(parts)-2])
	if err != nil {
		return time.Time{}, err
	}
	return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), nil
}

// formatDateForFilename formats date as YYYYMMDD for filename prefix
func formatDateForFilename(date time.Time) string {
	return date.Format("20060102")
}

// getObjectSize gets the actual size of an object by fetching its metadata
func getObjectSize(ctx context.Context, client objectstorage.ObjectStorageClient, namespace, bucketName, objectName string) (int64, error) {
	req := objectstorage.HeadObjectRequest{
		NamespaceName: &namespace,
		BucketName:    &bucketName,
		ObjectName:    &objectName,
	}

	resp, err := client.HeadObject(ctx, req)
	if err != nil {
		return 0, fmt.Errorf("failed to get object metadata for %s: %w", objectName, err)
	}

	if resp.ContentLength == nil {
		return 0, fmt.Errorf("content length not available for %s", objectName)
	}

	return *resp.ContentLength, nil
}

// listAllFocusReports lists all FOCUS reports
func listAllFocusReports(ctx context.Context, client objectstorage.ObjectStorageClient, namespace, bucketName string, days int) ([]objectstorage.ObjectSummary, error) {
	var allObjects []objectstorage.ObjectSummary
	var nextStart *string
	cutoff := time.Now().AddDate(0, 0, -days)

	for {
		req := objectstorage.ListObjectsRequest{
			NamespaceName: &namespace,
			BucketName:    &bucketName,
			Start:         nextStart,
			Limit:         common.Int(1000),
		}

		resp, err := client.ListObjects(ctx, req)
		if err != nil {
			return nil, fmt.Errorf("error listing objects: %w", err)
		}

		for _, obj := range resp.ListObjects.Objects {
			if obj.Name == nil {
				continue
			}
			name := *obj.Name
			if strings.Contains(name, "FOCUS") || strings.Contains(name, "FOCUS_REPORT") {
				objDate, err := parseDateFromName(name)
				if err != nil {
					log.Printf("Skipping object with invalid date format: %s", name)
					continue
				}
				if objDate.After(cutoff) {
					allObjects = append(allObjects, obj)
				}
			}
		}

		if resp.ListObjects.NextStartWith == nil || *resp.ListObjects.NextStartWith == "" {
			break
		}
		nextStart = resp.ListObjects.NextStartWith
	}

	return allObjects, nil
}

// downloadSingleFile downloads a single file with date prefix
func downloadSingleFile(ctx context.Context, client objectstorage.ObjectStorageClient, job Job, folder string) (OperationResult, error) {
	result := OperationResult{
		FileName:    path.Base(job.ObjectName),
		LastAttempt: time.Now(),
	}

	// Get actual file size using HeadObject
	size, err := getObjectSize(ctx, client, job.Namespace, job.BucketName, job.ObjectName)
	if err != nil {
		log.Printf("Warning: Could not get size for %s: %v", job.ObjectName, err)
		result.FileSize = 0
	} else {
		result.FileSize = size
	}

	// Extract date and create prefixed filename
	var datePrefix string
	if date, err := parseDateFromName(job.ObjectName); err == nil {
		result.ReportDate = date.Format("2006-01-02")
		datePrefix = formatDateForFilename(date) + "_"
	} else {
		datePrefix = "unknown_date_"
		result.ReportDate = "unknown"
	}

	// Create filename with date prefix
	prefixedFilename := datePrefix + path.Base(job.ObjectName)
	filePath := filepath.Join(folder, prefixedFilename)
	result.FileName = prefixedFilename // Update result with new filename

	// Skip if already downloaded
	if _, err := os.Stat(filePath); err == nil {
		result.Status = "Already exists"
		result.Downloaded = false
		return result, nil
	}

	// Download the file
	req := objectstorage.GetObjectRequest{
		NamespaceName: &job.Namespace,
		BucketName:    &job.BucketName,
		ObjectName:    &job.ObjectName,
	}

	resp, err := client.GetObject(ctx, req)
	if err != nil {
		result.Status = "Failed"
		result.Error = err.Error()
		return result, err
	}
	defer resp.Content.Close()

	outFile, err := os.Create(filePath)
	if err != nil {
		result.Status = "Failed"
		result.Error = err.Error()
		return result, err
	}
	defer outFile.Close()

	bytesCopied, err := io.Copy(outFile, resp.Content)
	if err != nil {
		result.Status = "Failed"
		result.Error = err.Error()
		return result, err
	}

	// Update with actual downloaded size
	result.FileSize = bytesCopied
	result.Status = "Success"
	result.Downloaded = true

	log.Printf("Downloaded %s (%d bytes) to %s", job.ObjectName, bytesCopied, filePath)
	return result, nil
}

// worker processes download jobs
func (wp *WorkerPool) worker(id int) {
	defer wp.wg.Done()
	
	for job := range wp.jobs {
		result, err := downloadSingleFile(wp.ctx, wp.client, job, wp.config.DownloadFolder)
		wp.results <- Result{Job: job, Result: result, Error: err}
	}
}

// NewWorkerPool creates a new worker pool
func NewWorkerPool(ctx context.Context, client objectstorage.ObjectStorageClient, config Config) *WorkerPool {
	return &WorkerPool{
		jobs:    make(chan Job, config.MaxWorkers*2),
		results: make(chan Result, config.MaxWorkers*2),
		config:  config,
		client:  client,
		ctx:     ctx,
	}
}

// Start begins processing with the specified number of workers
func (wp *WorkerPool) Start() {
	for i := 0; i < wp.config.MaxWorkers; i++ {
		wp.wg.Add(1)
		go wp.worker(i + 1)
	}
}

// AddJob adds a job to the queue
func (wp *WorkerPool) AddJob(job Job) {
	wp.jobs <- job
}

// WaitForCompletion waits for all workers to finish and closes channels
func (wp *WorkerPool) WaitForCompletion() {
	close(wp.jobs)
	wp.wg.Wait()
	close(wp.results)
}

func writeOperationReport(results []OperationResult, filename string) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header
	header := []string{
		"file_name",
		"file_size",
		"report_date",
		"status",
		"downloaded",
		"error",
		"last_attempt",
	}
	if err := writer.Write(header); err != nil {
		return err
	}

	// Write records
	for _, result := range results {
		record := []string{
			result.FileName,
			fmt.Sprintf("%d", result.FileSize),
			result.ReportDate,
			result.Status,
			strconv.FormatBool(result.Downloaded),
			result.Error,
			result.LastAttempt.Format(time.RFC3339),
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	workers := flag.Int("workers", 4, "Number of concurrent download workers")
	days := flag.Int("days", 7, "Number of past days to include in the report")
	downloadFolder := flag.String("download", "", "Folder to download reports (optional)")
	reportFile := flag.String("report", "download_report.csv", "Download operation report file")
	flag.Parse()

	config := Config{
		MaxWorkers: *workers,
		Days:       *days,
		DownloadFolder: *downloadFolder,
		ReportFile: *reportFile,
	}

	// Validate workers count
	if config.MaxWorkers < 1 {
		config.MaxWorkers = 1
	}
	if config.MaxWorkers > 16 {
		config.MaxWorkers = 16
		log.Printf("Warning: Limiting workers to 16 for safety")
	}

	provider := common.DefaultConfigProvider()
	client, err := objectstorage.NewObjectStorageClientWithConfigurationProvider(provider)
	if err != nil {
		log.Fatalf("Error creating Object Storage client: %v", err)
	}

	tenancyID, err := provider.TenancyOCID()
	if err != nil {
		log.Fatalf("Failed to read tenancy OCID from config: %v", err)
	}

	ctx := context.Background()
	namespace := "bling"
	bucketName := tenancyID

	// Create download directory if specified
	if config.DownloadFolder != "" {
		if err := os.MkdirAll(config.DownloadFolder, 0755); err != nil {
			log.Fatalf("Failed to create download folder %s: %v", config.DownloadFolder, err)
		}
	}

	// List all FOCUS reports
	objects, err := listAllFocusReports(ctx, client, namespace, bucketName, config.Days)
	if err != nil {
		log.Fatalf("Failed to list FOCUS reports: %v", err)
	}

	fmt.Printf("Found %d FOCUS reports in bucket %s\n", len(objects), bucketName)

	// Download reports if folder provided
	var downloadResults []OperationResult
	if config.DownloadFolder != "" {
		// Create worker pool
		pool := NewWorkerPool(ctx, client, config)
		pool.Start()

		// Start results collector
		var resultsMutex sync.Mutex
		var wgResults sync.WaitGroup

		wgResults.Add(1)
		go func() {
			defer wgResults.Done()
			for result := range pool.results {
				resultsMutex.Lock()
				downloadResults = append(downloadResults, result.Result)
				resultsMutex.Unlock()
				
				if result.Error != nil {
					log.Printf("Failed to download %s: %v", result.Job.ObjectName, result.Error)
				} else if result.Result.Status == "Success" {
					fmt.Printf("✓ %s → %s (%d bytes)\n",
						path.Base(result.Job.ObjectName),
						result.Result.FileName,
						result.Result.FileSize)
				}
			}
		}()

		// Add jobs to queue
		fmt.Printf("Starting %d workers to process %d files...\n", config.MaxWorkers, len(objects))
		startTime := time.Now()
		
		for _, obj := range objects {
			if obj.Name != nil {
				pool.AddJob(Job{
					ObjectName: *obj.Name,
					Namespace:  namespace,
					BucketName: bucketName,
				})
			}
		}

		// Wait for completion
		pool.WaitForCompletion()
		wgResults.Wait()
		
		totalTime := time.Since(startTime)
		fmt.Printf("Download completed in %v\n", totalTime)

		// Write operation report
		if err := writeOperationReport(downloadResults, config.ReportFile); err != nil {
			log.Fatalf("Failed to write operation report: %v", err)
		}
		fmt.Printf("Download operation report generated: %s\n", config.ReportFile)
		fmt.Printf("Reports downloaded successfully to folder: %s\n", config.DownloadFolder)
	}

	// Generate summary CSV with correct sizes
	type Report struct {
		Name string
		Size int64
		Date time.Time
	}

	var reports []Report
	for _, obj := range objects {
		if obj.Name == nil {
			continue
		}
		name := *obj.Name
		
		// Get actual size using HeadObject
		size, err := getObjectSize(ctx, client, namespace, bucketName, name)
		if err != nil {
			log.Printf("Warning: Could not get size for %s: %v", name, err)
			size = 0
		}
		
		date, err := parseDateFromName(name)
		if err != nil {
			continue
		}
		reports = append(reports, Report{Name: name, Size: size, Date: date})
	}

	// Sort descending by Date
	sort.Slice(reports, func(i, j int) bool {
		return reports[i].Date.After(reports[j].Date)
	})

	// Write CSV
	csvFile, err := os.Create("oci_focus_reports.csv")
	if err != nil {
		log.Fatalf("Error creating CSV file: %v", err)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	writer.Write([]string{"bucket_name", "object_name", "size_bytes", "report_date", "tenancy_ocid"})

	for _, r := range reports {
		writer.Write([]string{
			bucketName,
			path.Base(r.Name),
			fmt.Sprintf("%d", r.Size),
			r.Date.Format("2006-01-02"),
			bucketName,
		})
	}

	fmt.Printf("CSV file generated successfully: oci_focus_reports.csv (%d reports)\n", len(reports))
}
