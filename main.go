package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strings"

	"log/slog"
)


func main() {
	
	// ----- SECTION 1: INITIAL SETUP -----
	// set up logger using GO's built-in slog package 
	programLevel := new(slog.LevelVar)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel}))
	slog.SetDefault(logger)

	// read required environment variables: where to authenticate token, download files, etc
	integrationID := os.Getenv("INTEGRATION_ID")
	logger.Info(integrationID)
	outputDir := os.Getenv("OUTPUT_DIR")
	sessionToken := os.Getenv("SESSION_TOKEN")
	apiHost := os.Getenv("PENNSIEVE_API_HOST")
	apiHost2 := os.Getenv("PENNSIEVE_API_HOST2")

	// makes a web request to the API to get the integration details
	// integration contains details abou the dataset and package IDs to be downloaded
	// the response would come back as raw data that needs to be parsed (see line below)
	integrationResponse, err := getIntegration(apiHost2, integrationID, sessionToken)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Printing intergration response")
	fmt.Println(string(integrationResponse))

	// parse integration response
	// use json.unmarshal to convert the raw response into structured format that the program can work with
	var integration Integration // creates a variable "integration" of type "Integration" (customized later)
	if err := json.Unmarshal(integrationResponse, &integration); err != nil {
		logger.ErrorContext(context.Background(), err.Error())
	}
	fmt.Println("Printing intergration")
	fmt.Println(integration)

	// get presigned URLs for the package IDs listed in the integration (previsouly itegration only listed package IDs but not URLs)
	manifest, err := getPresignedUrls(apiHost, getPackageIds(integration.PackageIDs), sessionToken)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Println("Printing manifest")
	fmt.Println(string(manifest))

	// parse manifest response to get a list of files with their names and download URLs
	var payload Manifest // creates a variable "payload" of type "Manifest" (customized later)
	if err := json.Unmarshal(manifest, &payload); err != nil {
		logger.ErrorContext(context.Background(), err.Error())
	}
	fmt.Println("Printing payload.Data")
	fmt.Println(payload.Data)
	
	

	// ----- SECTION 2: CSV CREATION -----
	// SZ ADDED FOR DEBUGGING - OCT 09 2025
	// Create CSV file for the folder structure mapping
	csvFile, err := os.Create(outputDir + "/file_paths.csv")
	if err != nil {
		logger.Error("Failed to create CSV file", slog.String("error", err.Error()))
		return
	}
	defer csvFile.Close()
	csvFile.WriteString("filename,source_path,target_path\n") // write CSV header

	for _, d := range payload.Data {
		// Generate CSV data for this file
		fileName := d.FileName // name of the file
		sourcePath := outputDir + "/" + d.FileName   // full path where the file will be downloaded
		var targetPath string
		if len(d.Path) > 0 {
			targetPath = strings.Join(d.Path, "/")  // reconstructed folder structure
		} else {
			targetPath = "."  // root directory
		}
		csvFile.WriteString(fmt.Sprintf("%s,%s,%s\n", fileName, sourcePath, targetPath)) // Write to CSV file
	}


	// ----- SECTION 3: FILE DOWNLOAD -----
	// SZ ADDED FOR DEBUGGING - OCT 09 2025
	// copy files into input directory WITH FOLDER STRUCTURE
	// loop through the parsed manifest data and use wget to download each file maintaining hierarchy
	for _, d := range payload.Data {
		
		// Construct the target directory path based on the folder structure
		var targetDir string
		if len(d.Path) > 0 {
			// Join the path elements to create subdirectory structure
			targetDir = outputDir + "/" + strings.Join(d.Path, "/") // e.g. /mnt/efs/input/xxxxxxxx/twolayer/onelayer
		} else {
			// If no path, download to root of outputDir
			targetDir = outputDir // e.g. /mnt/efs/input/xxxxxxxx for file with no subfolder
		}
		
		// Create the directory structure if it doesn't exist
		if err := os.MkdirAll(targetDir, 0755); err != nil {
			logger.Error("Failed to create directory structure",
				slog.String("directory", targetDir),
				slog.String("error", err.Error()))
			continue // Skip this file and continue with next
		}
		
		// cmd to copy files into input dir
		// Full path where the file will be saved
		fullFilePath := targetDir + "/" + d.FileName // e.g. /mnt/efs/input/xxxxxxxx/twolayer/onelayer/df1.csv
		fmt.Printf("Downloading to: %s\n", fullFilePath)
		
		// cmd to download file into the structured directory
		cmd := exec.Command("wget", "-v", "-O", fullFilePath, d.Url) // Note: we don't set cmd.Dir because we're using absolute paths
		var out strings.Builder
		var stderr strings.Builder
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		
		// If there was an error, log it
		if err := cmd.Run(); err != nil {
			logger.Error("Download failed",
				slog.String("file", d.FileName),
				slog.String("error", stderr.String()))
			continue
		}
		
		// Print stdout content
		fmt.Println("Stdout output:")
		fmt.Println(out.String())
		
		// Print stderr content (wget verbose output)
		fmt.Println("Stderr output (verbose output):")
		fmt.Println(stderr.String())
		
		fmt.Printf("✓ Successfully downloaded: %s\n\n", fullFilePath)
	}

	// // copy files into input directory
	// // loop through the pasrsed manifest data and use wget to download each file using their filename and Url
	// for _, d := range payload.Data {
		

	// 	// SZ ADDED FOR DEBUGGING - SEP 30 2025
	// 	// Print all available data for each file
	// 	fmt.Println("=== File Details ===")
	// 	fmt.Println("FileName:", d.FileName) 
	// 	fmt.Println("Path:", d.Path)
	// 	fmt.Println("===================")
		

	// 	// cmd to copy files into input dir
	// 	cmd := exec.Command("wget", "-v", "-O", d.FileName, d.Url) // create a command for download 
	// 	cmd.Dir = outputDir // set the working dir to outputDir so the downladed files would go there
	// 	var out strings.Builder // creates a variable "out" with property as "strings.Builder" - like a notepad where you can keep adding text 
	// 	var stderr strings.Builder
	// 	cmd.Stdout = &out
	// 	cmd.Stderr = &stderr
	// 	downloadErr := cmd.Run() // execute the command

	// 	// print stdout content
	// 	stdoutContent := out.String()
	// 	fmt.Println("Stdout output:")
	// 	fmt.Println(stdoutContent)

	// 	// print or log stderr content
	// 	stderrContent := stderr.String()
	// 	fmt.Println("Stderr output (verbose output):")
	// 	fmt.Println(stderrContent)

	// 	// If there was an error, log it
	// 	if downloadErr != nil {
	// 		logger.Error(downloadErr.Error(),
	// 			slog.String("error", stderrContent))
	// 	}
	// }

}



type Packages struct {
	NodeIds []string `json:"nodeIds"`
}

type Manifest struct {
	Data []ManifestData `json:"data"`
}

type ManifestData struct {
	NodeId   string   `json:"nodeId"`
	FileName string   `json:"fileName"`
	Path     []string `json:"path"`
	Url      string   `json:"url"`
}

type Integration struct {
	Uuid          string      `json:"uuid"`
	ApplicationID int64       `json:"applicationId"`
	DatasetNodeID string      `json:"datasetId"`
	PackageIDs    []string    `json:"packageIds"`
	Params        interface{} `json:"params"`
}

func getPresignedUrls(apiHost string, packages Packages, sessionToken string) ([]byte, error) {
	url := fmt.Sprintf("%s/packages/download-manifest?api_key=%s", apiHost, sessionToken)
	b, err := json.Marshal(packages)
	if err != nil {
		return nil, err
	}
	fmt.Println(string(b))

	payload := strings.NewReader(string(b))

	req, _ := http.NewRequest("POST", url, payload)

	req.Header.Add("accept", "*/*")
	req.Header.Add("content-type", "application/json")

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return body, nil
}

func getPackageIds(packageIds []string) Packages {
	return Packages{
		NodeIds: packageIds,
	}
}

func getIntegration(apiHost string, integrationId string, sessionToken string) ([]byte, error) {
	url := fmt.Sprintf("%s/integrations/%s", apiHost, integrationId)

	req, _ := http.NewRequest("GET", url, nil)

	req.Header.Add("accept", "application/json")
	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", sessionToken))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer res.Body.Close()
	body, _ := io.ReadAll(res.Body)

	return body, nil
}
