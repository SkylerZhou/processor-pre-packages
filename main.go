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
	// set up logger using GO's built-in slog package 
	programLevel := new(slog.LevelVar)
	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: programLevel}))
	slog.SetDefault(logger)

	// read required environment variables: where to authenticate token, download files, etc
	integrationID := os.Getenv("INTEGRATION_ID")
	logger.Info(integrationID)
	inputDir := os.Getenv("INPUT_DIR")
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
	
	// SZ ADDED FOR DEBUGGING - SEP 30 2025
	// Create CSV file for the folder structure mapping
	csvFile, err := os.Create(inputDir + "/file_paths.csv")
	if err != nil {
		logger.Error("Failed to create CSV file", slog.String("error", err.Error()))
		return
	}
	defer csvFile.Close()
	csvFile.WriteString("source_path,target_path\n") // write CSV header
	
	// copy files into input directory
	// loop through the pasrsed manifest data and use wget to download each file using their filename and Url
	for _, d := range payload.Data {
		
		// SZ ADDED FOR DEBUGGING - SEP 30 2025
		// Print all available data for each file
		fmt.Println("=== File Details ===")
		fmt.Println("FileName:", d.FileName) 
		fmt.Println("Path:", d.Path)
		fmt.Println("===================")
		
		// SZ ADDED FOR DEBUGGING - SEP 30 2025
		// Generate CSV data for this file
		sourcePath := inputDir + "/" + d.FileName  // where the file will be downloaded
		var targetPath string
		if len(d.Path) > 0 {
			targetPath = strings.Join(d.Path, "/")  // reconstructed folder structure
		} else {
			targetPath = "."  // root directory
		}
		csvFile.WriteString(fmt.Sprintf("%s,%s\n", sourcePath, targetPath)) // Write to CSV file
		
		cmd := exec.Command("wget", "-v", "-O", d.FileName, d.Url) // create a command for download 
		cmd.Dir = inputDir // set the working dir to inputDir so the downladed files would go there
		var out strings.Builder // creates a variable "out" with property as "strings.Builder" - like a notepad where you can keep adding text 
		var stderr strings.Builder
		cmd.Stdout = &out
		cmd.Stderr = &stderr
		err := cmd.Run() // execute the command

		// print stdout content
		stdoutContent := out.String()
		fmt.Println("Stdout output:")
		fmt.Println(stdoutContent)

		// print or log stderr content
		stderrContent := stderr.String()
		fmt.Println("Stderr output (verbose output):")
		fmt.Println(stderrContent)

		// If there was an error, log it
		if err != nil {
			logger.Error(err.Error(),
				slog.String("error", stderrContent))
		}
	}

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
