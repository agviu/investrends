package cmd

import (
	"context"
	"encoding/base64"
	"log"
	"os"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"
	"github.com/spf13/cobra"
	"google.golang.org/api/option"
)

// filePath holds the path to the file we want to upload.
var filePath string

// firebaseKey holds the path to the Firebase service account key.
var firebaseKey string

// uploadCmd represents the upload command to Cloud Firestore.
var uploadCmd = &cobra.Command{
	Use:   "upload",
	Short: "Uploads a file to Cloud Firestore",
	Long: `This command uploads a file to Cloud Firestore using the Firebase Admin SDK.
You must specify the file path and the Firebase service account key file.`,
	Run: func(cmd *cobra.Command, args []string) {
		// Create a new context for the Firestore operation.
		ctx := context.Background()

		// Initialize the Firestore client.
		firestoreClient, err := initFirestore(ctx, firebaseKey)
		if err != nil {
			log.Fatalf("Failed to initialize Firestore: %v", err)
		}
		defer firestoreClient.Close()

		// Call the function to upload the file to Firestore.
		if err := uploadFileToFirestore(ctx, firestoreClient, filePath); err != nil {
			log.Fatalf("Failed to upload file to Firestore: %v", err)
		}
		log.Println("File uploaded to Firestore successfully")
	},
}

func init() {
	rootCmd.AddCommand(uploadCmd)

	// Set up the command-line flags.
	uploadCmd.Flags().StringVarP(&filePath, "file", "f", "", "Path to the file to upload")
	uploadCmd.Flags().StringVarP(&firebaseKey, "key", "k", "", "Path to the Firebase service account key file")

	// Make sure both flags are provided by marking them as required.
	uploadCmd.MarkFlagRequired("file")
	uploadCmd.MarkFlagRequired("key")
}

// initFirestore initializes the Firestore client using the service account key.
func initFirestore(ctx context.Context, serviceAccountPath string) (*firestore.Client, error) {
	// Set up the admin SDK with the service account key file.
	opt := option.WithCredentialsFile(serviceAccountPath)
	app, err := firebase.NewApp(ctx, nil, opt)
	if err != nil {
		return nil, err
	}

	// Obtain the Firestore client from the initialized app.
	firestoreClient, err := app.Firestore(ctx)
	if err != nil {
		return nil, err
	}
	return firestoreClient, nil
}

// uploadFileToFirestore uploads the content of the file at filePath to Firestore.
func uploadFileToFirestore(ctx context.Context, firestoreClient *firestore.Client, filePath string) error {
	// Read the file content from the file at filePath.
	fileContent, err := os.ReadFile(filePath)
	if err != nil {
		return err
	}

	// Since Firestore does not directly store binary data,
	// we encode the file content to a Base64 string.
	encodedContent := base64.StdEncoding.EncodeToString(fileContent)

	// Create a new document in the 'files' collection with the encoded file content.
	_, _, err = firestoreClient.Collection("files").Add(ctx, map[string]interface{}{
		"content": encodedContent, // The Base64-encoded file content.
	})
	if err != nil {
		return err
	}

	return nil
}
