import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.nio.charset.StandardCharsets;
/*
    DEVELOPED BY BYTE BUILDING LLC
    CONTACT US AT BYTE-BUILDING.COM

    FILE UPLOAD PROGRAM TO PROCESS AND CREATE NEW CSV FILES
    THE PURPOSE OF THIS IS TO STEAMLINE INVENTORY MANAGEMENT
*/
public class FileUploadServer {
    // MAIN PROCESS TO START EXECUTION OF PROGRAM
    public static void main(String[] args) throws IOException {
        System.out.println("-- STARTING PROGRAM --");
        startServer();
    }

    // LISTEN ON PORT 8080
    private static void startServer() throws IOException {
        System.out.println("-- PROCESS STARTING ON PORT 8080 --");
        ServerSocketChannel serverSocketChannel = null;
        try {
            serverSocketChannel = ServerSocketChannel.open();
            serverSocketChannel.socket().bind(new InetSocketAddress(8080));
            serverSocketChannel.configureBlocking(true);
            // START PROCESS TO LISTEN ON PORT
            while (true) {
                try {
                    SocketChannel clientChannel = serverSocketChannel.accept();
                    new Thread(new ClientHandler(clientChannel)).start();
                } 
                catch (IOException e) {
                    System.err.println("Failed to accept a connection: " + e.getMessage());
                }
            }
        } 
        catch (IOException e) {
            System.err.println("Failed to start the server on port 8080: " + e.getMessage());
        } 
        finally {
            if (serverSocketChannel != null && serverSocketChannel.isOpen()) {
                try {
                    serverSocketChannel.close();
                    System.out.println("Server socket closed.");
                } 
                catch (IOException e) {
                    System.err.println("Failed to close server socket: " + e.getMessage());
                }
            }
        }
    }

    static class ClientHandler implements Runnable {
        private SocketChannel clientChannel;

        public ClientHandler(SocketChannel clientChannel) {
            this.clientChannel = clientChannel;
        }

        // Method to serve the GET request and render the HTML form
        private void handleGETRequest() throws IOException {
            System.out.println("-- SERVING GET REQUEST --");

            // Using StringBuilder for better performance and readability
            StringBuilder response = new StringBuilder();
            try {
                response.append("HTTP/1.1 200 OK\r\n")
                        .append("Content-Type: text/html; charset=UTF-8\r\n\r\n")
                        .append("<html>")
                        .append("<head>")
                        .append("<style>")
                        .append("body { font-family: Arial, sans-serif; }")
                        .append(".form-container {")
                        .append("   display: inline-block;")
                        .append("   width: 45%;")
                        .append("   margin: 20px;")
                        .append("   vertical-align: top;")
                        .append("}")
                        .append(".center-text { text-align: center; }")
                        .append(".left-text { text-align: left; }")
                        .append("</style>")
                        .append("</head>")
                        .append("<body>")
                        .append("<h1 class=\"center-text\">SMOKE TOKES</h1>")
                        .append("<h2 class=\"center-text\">FILE CREATION PROCESS</h2>")
                        .append("<p class=\"center-text\">CREATE NEW CSV FILES USED BY ZOHO AND SHOPIFY</p>")
                        .append("<p class=\"center-text\">UPLOAD CSV FILES LOCATED ON YOUR COMPUTER</p>")
                        .append("<div class=\"center-text\">")
                        .append("<div class=\"form-container\">")
                        .append("<h3>B2B PRICE LIST TO ZOHO</h3>")
                        .append("<form action=\"/form1\" method=\"post\" class=\"left-text\" enctype=\"multipart/form-data\">")
                        .append("EMPLOYEE NAME: <input type=\"text\" name=\"employeeName\"><br><br>")
                        .append("B2B PRICE LIST FILE: <input type=\"file\" name=\"b2bPriceListFile\"><br><br>")
                        .append("ZOHO PRICE LIST FILE: <input type=\"file\" name=\"zohoPriceListFile\"><br><br>")
                        .append("<input type=\"submit\" value=\"Submit\">")
                        .append("</form>")
                        .append("</div>")
                        .append("<div class=\"form-container\">")
                        .append("<h3>BARCODE LIST TO ZOHO</h3>")
                        .append("<form action=\"/form2\" method=\"post\" class=\"left-text\" enctype=\"multipart/form-data\">")
                        .append("EMPLOYEE NAME: <input type=\"text\" name=\"employeeName\"><br><br>")
                        .append("SHOPIFY PRODUCT FILE: <input type=\"file\" name=\"shopifyProductFile\"><br><br>")
                        .append("ZOHO EXPORT FILE: <input type=\"file\" name=\"zohoExportFile\"><br><br>")
                        .append("<input type=\"submit\" value=\"Submit\">")
                        .append("</form>")
                        .append("</div>")
                        .append("</div>")
                        .append("</body>")
                        .append("</html>");
                
                clientChannel.write(ByteBuffer.wrap(response.toString().getBytes()));
            }
            catch (IOException e) {
                // Log the error
                System.err.println("Error while handling GET request: " + e.getMessage());

                // Send an error response to the client
                String errorMessage = "HTTP/1.1 500 Internal Server Error\r\n" +
                                      "Content-Type: text/plain; charset=UTF-8\r\n\r\n" +
                                      "An error occurred while processing your request.";
                try {
                    clientChannel.write(ByteBuffer.wrap(errorMessage.getBytes()));
                } 
                catch (IOException ioException) {
                    System.err.println("Failed to send error response to client: " + ioException.getMessage());
                }
            } 
            finally {
                // Ensure the client channel is closed
                if (clientChannel != null && clientChannel.isOpen()) {
                    try {
                        clientChannel.close();
                        System.out.println("Client connection closed.");
                    } catch (IOException e) {
                        System.err.println("Failed to close client connection: " + e.getMessage());
                    }
                }
            }
        }

        // POST REQUEST METHOD TO HANDLE INPUT FORM
        private void handlePOSTRequest(String request) {
            System.out.println("-- HANDLE POST REQUEST --");
            try 
            {
                // Extract the request URI to determine the form action
                String formAction = extractFormActionFromRequestURI(request);

                System.out.println("FORM ACTION: " + formAction);

                // Route to the appropriate handler based on the form action
                if (formAction.equals("form1")) {
                    System.out.println("-- PROCESSING FORM 1 --");
                    processFormOne(request);
                } 
                else if (formAction.equals("form2")) {
                    System.out.println("-- PROCESSING FORM 2 --");
                    processFormTwo(request);
                } 
                else {
                    // If the action is not recognized, serve the GET request to show the form again
                    handleGETRequest();
                }
            } 
            catch (Exception e) {
                // Log the error
                System.err.println("Error while handling POST request: " + e.getMessage());

                // Send an error response to the client
                String errorMessage = "HTTP/1.1 500 Internal Server Error\r\n" +
                                      "Content-Type: text/plain; charset=UTF-8\r\n\r\n" +
                                      "An error occurred while processing your request.";
                try {
                    clientChannel.write(ByteBuffer.wrap(errorMessage.getBytes()));
                } 
                catch (IOException ioException) {
                    System.err.println("Failed to send error response to client: " + ioException.getMessage());
                }
            } 
            finally {
                // Ensure the client channel is closed
                if (clientChannel != null && clientChannel.isOpen()) {
                    try {
                        clientChannel.close();
                        System.out.println("Client connection closed.");
                    } 
                    catch (IOException e) {
                        System.err.println("Failed to close client connection: " + e.getMessage());
                    }
                }
            }
        }

        private String extractFormActionFromRequestURI(String request) {
            // Ensure the request is not null or empty
            if (request == null || request.isEmpty()) {
                System.err.println("Request is null or empty.");
                return "";
            }

            // The first line of the HTTP request contains the method and URI, e.g., "POST /form1 HTTP/1.1"
            String[] requestLines = request.split("\r\n");
            if (requestLines.length == 0) {
                System.err.println("Request is malformed: no lines found.");
                return "";
            }

            String requestLine = requestLines[0]; // The request line is the first line
            String[] requestParts = requestLine.split(" ");
            
            // Check that the request line contains at least the method and URI
            if (requestParts.length < 2) {
                System.err.println("Request line is malformed: " + requestLine);
                return "";
            }

            // The URI part of the request line, which includes the action, e.g., "/form1"
            String requestURI = requestParts[1];

            // Log the request URI for debugging purposes
            System.out.println("Extracted request URI: " + requestURI);

            // Extract the form action from the URI
            switch (requestURI) {
                case "/form1":
                    return "form1";
                case "/form2":
                    return "form2";
                default:
                    // Log unsupported actions for further analysis
                    System.err.println("Unsupported request URI: " + requestURI);
                    return "";
            }
        }


        private void processFormOne(String request) throws IOException {
            System.out.println("-- INSIDE FORM 1 METHOD --");
            
            try {
                // Parse the multipart form data to extract form fields
                 Map<String, String> formFields = parseMultipartFormData(request);

                // Validate form fields
                String firstName = formFields.get("firstName");
                String b2bPriceListFilePath = formFields.get("b2bPriceListFile");
                String zohoPriceListFilePath = formFields.get("zohoPriceListFile");


                // Check if any field is null or empty
                if (b2bPriceListFilePath == null || b2bPriceListFilePath.isEmpty() || 
                    zohoPriceListFilePath == null || zohoPriceListFilePath.isEmpty()) {
                    throw new IllegalArgumentException("Missing or empty form fields.");
                }

                // Log the extracted file names
                System.out.println("B2B Price List File: " + b2bPriceListFilePath);
                System.out.println("Zoho Price List File: " + zohoPriceListFilePath);

                // READ AND PROCESS USER UPLOADED FILES
                // Validate file names and read/process files
                processUploadedFileOne(b2bPriceListFilePath, zohoPriceListFilePath);

                // Build and send HTML response after successful processing
                StringBuilder responseBuilder = new StringBuilder();
                responseBuilder.append("HTTP/1.1 200 OK\r\n");
                responseBuilder.append("Content-Type: text/html; charset=UTF-8\r\n\r\n");
                responseBuilder.append("<html><body>");
                responseBuilder.append("<h3>B2B PRICE LIST TO ZOHO</h3>");
                responseBuilder.append("<p>EMPLOYEE NAME: ").append(escapeHtml(firstName)).append("</p><br>");
                responseBuilder.append("<p>UPLOADED FILES:</p>");
                responseBuilder.append("<p>B2B PRICE LIST FILE: ").append(escapeHtml(b2bPriceListFilePath)).append("</p><br>");
                responseBuilder.append("<p>ZOHO PRODUCT FILE: ").append(escapeHtml(zohoPriceListFilePath)).append("</p><br>");
                responseBuilder.append("</body></html>");
                String response = responseBuilder.toString();

                clientChannel.write(ByteBuffer.wrap(response.getBytes()));

            } 
            catch (IllegalArgumentException e) {
                // Handle invalid form input
                sendErrorResponse("400 Bad Request", "Invalid form data: " + e.getMessage());

            } 
            catch (IOException e) {
                // Handle IO errors
                sendErrorResponse("500 Internal Server Error", "An error occurred while processing the files: " + e.getMessage());

            } 
            catch (Exception e) {
                // Handle unexpected errors
                sendErrorResponse("500 Internal Server Error", "An unexpected error occurred: " + e.getMessage());

            } 
            finally {
                // Close the client channel if open
                if (clientChannel != null && clientChannel.isOpen()) {
                    try {
                        clientChannel.close();
                        System.out.println("Client connection closed.");
                    } 
                    catch (IOException e) {
                        System.err.println("Failed to close client connection: " + e.getMessage());
                    }
                }
            }
        }

        private void processFormTwo(String request) throws IOException {
            System.out.println("-- INSIDE FORM 2 METHOD --");
            
            try {
                // Parse the multipart form data to extract form fields
                Map<String, String> formFields = parseMultipartFormData(request);
                // Validate form fields
                String firstName = formFields.get("firstName");
                String shopifyProductFilePath = formFields.get("shopifyProductFile");
                String zohoExportFilePath = formFields.get("zohoExportFile");

                // Check if any required field is null or empty
                if (shopifyProductFilePath == null || shopifyProductFilePath.isEmpty() || 
                    zohoExportFilePath == null || zohoExportFilePath.isEmpty()) {
                    throw new IllegalArgumentException("Missing or empty form fields.");
                }

                // Log the extracted field values
                System.out.println("First Name: " + firstName);
                System.out.println("Shopify Product File: " + shopifyProductFilePath);
                System.out.println("Zoho Export File: " + zohoExportFilePath);

                // READ AND PROCESS USER UPLOADED FILES
                // Validate file names and read/process files
                processUploadedFileTwo(shopifyProductFilePath, zohoExportFilePath);

                // Build and send HTML response after successful processing
                StringBuilder responseBuilder = new StringBuilder();
                responseBuilder.append("HTTP/1.1 200 OK\r\n");
                responseBuilder.append("Content-Type: text/html; charset=UTF-8\r\n\r\n");
                responseBuilder.append("<html><body>");
                responseBuilder.append("<h3>BARCODE TO ZOHO</h3>");
                responseBuilder.append("<p>EMPLOYEE NAME: ").append(escapeHtml(firstName)).append("</p><br>");
                responseBuilder.append("<p>UPLOADED FILES:</p>");
                responseBuilder.append("<p>SHOPIFY PRODUCT FILE: ").append(escapeHtml(shopifyProductFilePath)).append("</p><br>");
                responseBuilder.append("<p>ZOHO EXPORT FILE: ").append(escapeHtml(zohoExportFilePath)).append("</p><br>");
                responseBuilder.append("</body></html>");
                String response = responseBuilder.toString();

                clientChannel.write(ByteBuffer.wrap(response.getBytes()));

            } 
            catch (IllegalArgumentException e) {
                // Handle invalid form input
                sendErrorResponse("400 Bad Request", "Invalid form data: " + e.getMessage());

            } 
            catch (IOException e) {
                // Handle IO errors
                sendErrorResponse("500 Internal Server Error", "An error occurred while processing the files: " + e.getMessage());

            } 
            catch (Exception e) {
                // Handle unexpected errors
                sendErrorResponse("500 Internal Server Error", "An unexpected error occurred: " + e.getMessage());

            } 
            finally {
                // Close the client channel if open
                if (clientChannel != null && clientChannel.isOpen()) {
                    try {
                        clientChannel.close();
                        System.out.println("Client connection closed.");
                    } 
                    catch (IOException e) {
                        System.err.println("Failed to close client connection: " + e.getMessage());
                    }
                }
            }
        }

        // Helper method to send an error response to the client
        private void sendErrorResponse(String status, String message) {
            try {
                String response = "HTTP/1.1 " + status + "\r\n" +
                                  "Content-Type: text/html; charset=UTF-8\r\n\r\n" +
                                  "<html><body><h3>" + status + "</h3><p>" + escapeHtml(message) + "</p></body></html>";
                clientChannel.write(ByteBuffer.wrap(response.getBytes()));
            } 
            catch (IOException e) {
                System.err.println("Failed to send error response: " + e.getMessage());
            }
        }

         // Helper method to escape HTML content
        private String escapeHtml(String input) {
            if (input == null) {
                return "";
            }
            return input.replace("&", "&amp;")
                        .replace("<", "&lt;")
                        .replace(">", "&gt;")
                        .replace("\"", "&quot;")
                        .replace("'", "&#39;");
        }
        private Map<String, String> parseMultipartFormData(String request) throws IOException {
            Map<String, String> fileNames = new HashMap<>();
            
            // Split the headers from the body
            int bodyStartIndex = request.indexOf("\r\n\r\n") + 4;
            String headers = request.substring(0, bodyStartIndex);
            String body = request.substring(bodyStartIndex);

            // Extract the boundary from the Content-Type header
            String boundary = null;
            String[] headerLines = headers.split("\r\n");
            for (String header : headerLines) {
                if (header.contains("Content-Type: multipart/form-data;")) {
                    int boundaryIndex = header.indexOf("boundary=");
                    if (boundaryIndex != -1) {
                        boundary = header.substring(boundaryIndex + "boundary=".length());
                        boundary = "--" + boundary.trim(); // Add "--" as per HTTP specification
                        break;
                    }
                }
            }

            if (boundary == null) {
                throw new IOException("No boundary found in multipart/form-data");
            }

            // Split the body by the boundary to get the parts
            String[] parts = body.split(boundary);

            for (String part : parts) {
                if (part.contains("Content-Disposition: form-data;")) {
                    BufferedReader partReader = new BufferedReader(new StringReader(part));

                    String contentDisposition = null;
                    String line;

                    // Read lines to find the Content-Disposition header
                    while ((line = partReader.readLine()) != null) {
                        if (line.startsWith("Content-Disposition:")) {
                            contentDisposition = line;
                            break;
                        }
                    }

                    if (contentDisposition == null) {
                        continue; // Skip if no Content-Disposition header is found
                    }

                    // Check if this part is a file
                    if (contentDisposition.contains("filename=\"")) {
                        String fileName = extractFileName(contentDisposition);

                        // Determine the field name to use as the key in the map
                        String fieldName = extractFieldName(contentDisposition);
                        fileNames.put(fieldName, fileName);
                    }
                }
            }

            return fileNames;
        }


        private String extractFileName(String contentDisposition) {
            // Extract the file name from the Content-Disposition header
            String[] elements = contentDisposition.split(";");
            for (String element : elements) {
                if (element.trim().startsWith("filename=\"")) {
                    String fileName = element.substring(element.indexOf("filename=\"") + 10, element.length() - 1);
                    return URLDecoder.decode(fileName, StandardCharsets.UTF_8);
                }
            }
            return null;
        }

        private String extractFieldName(String contentDisposition) {
            // Extract the field name from the Content-Disposition header
            String[] elements = contentDisposition.split(";");
            for (String element : elements) {
                if (element.trim().startsWith("name=\"")) {
                    return element.substring(element.indexOf("name=\"") + 6, element.length() - 1);
                }
            }
            return null;
        }

        @Override
        public void run() {
            try {
                // STREAM INPUT DATA FROM POST REQUEST
                BufferedReader reader = new BufferedReader(new InputStreamReader(clientChannel.socket().getInputStream()));
                StringBuilder requestBuilder = new StringBuilder();

                // READ THE REQUEST LINE BY LINE
                String line;
                while ((line = reader.readLine()) != null && !line.isEmpty()) {
                    requestBuilder.append(line).append("\r\n");
                }

                // APPEND EMPTY LINE TO SEPARATE HEADER AND BODY
                requestBuilder.append("\r\n");

                // READ THE BODY IF EXISTS
                while (reader.ready()) {
                    requestBuilder.append((char) reader.read());
                }

                String request = requestBuilder.toString();

                // SPLIT REQUEST INTO LINES
                String[] requestLines = request.split("\r\n");
                if (requestLines.length > 0) {
                    String method = requestLines[0].split(" ")[0];
                    String path = requestLines[0].split(" ")[1];

                    // HANDLE REQUEST BASED ON HTTP METHOD
                    if (method.equals("GET")) {
                        handleGETRequest();
                    } else if (method.equals("POST")) {
                        handlePOSTRequest(request);
                    }
                }
            } 
            catch (IOException e) {
                // Handle the exception gracefully
                System.err.println("Error occurred while processing request: " + e.getMessage());
            } 
            finally {
                try {
                    // Close the client channel in a finally block to ensure it always gets closed
                    if (clientChannel != null && clientChannel.isOpen()) {
                        clientChannel.close();
                    }
                } 
                catch (IOException e) {
                    System.err.println("Error occurred while closing client channel: " + e.getMessage());
                }
            }
        }

        // PROCESS THE 2 USER UPLOADED FILES
        // FILE 1: B2B PRICE LIST FILE
        // FILE 2: ZOHO PRODUCT FILE
        private static void processUploadedFileOne(String priceListCSV, String productListCSV) {
            try {
                System.out.println("-- STARTING MERGING AND WRITING FOR FORM 1 --");
                String outputDistributorFile = "merged_distributor_prices.csv";
                String outputJobberFile = "merged_jobber_prices.csv";
                String outputMemberFile = "merged_member_prices.csv";

                // READ 3 DISTINCT PRICES FROM PRICE LIST FILE AND STORE IN SEPARATE HASH MAPS TIED TO SKU NUMBER
                Map<String, String> distributorPrices = readPriceList(priceListCSV, "distributor");
                Map<String, String> jobberPrices = readPriceList(priceListCSV, "jobber");
                Map<String, String> memberPrices = readPriceList(priceListCSV, "member");
                
                // MATCH PRICES FROM HASH MAP TO PRODUCT LIST FILE USING SHARED SKU NUMBER
                System.out.println("-- MERGING AND WRITING DISTRUBUTOR PRICES FILE --");
                mergeAndWriteFormOne(productListCSV, outputDistributorFile, distributorPrices);
                System.out.println("-- MERGING AND WRITING JOBBER PRICES FILE --");
                mergeAndWriteFormOne(productListCSV, outputJobberFile, jobberPrices);
                System.out.println("-- MERGING AND WRITING MEMBER PRICES FILE --");
                mergeAndWriteFormOne(productListCSV, outputMemberFile, memberPrices);
                System.out.println("-- ENDING MERGING AND WRITING --");
            } 
             catch (IOException e) {
                // Handle IO exception gracefully
                System.err.println("Error occurred while processing uploaded files: " + e.getMessage());
            }
        }

        // PROCESS THE 2 USER UPLOADED FILES
        // FILE 1: B2B PRICE LIST FILE
        // FILE 2: ZOHO PRODUCT FILE
        private static void processUploadedFileTwo(String productExportCSV, String zohoUploadCSV) {
            try {
                System.out.println("-- STARTING MERGING AND WRITING FORM 2 --");
                String outputZohoBarcodeFile = "merged_zoho_barcode.csv";


                // READ 3 DISTINCT PRICES FROM PRICE LIST FILE AND STORE IN SEPARATE HASH MAPS TIED TO SKU NUMBER
                Map<String, String> productExportBarcodes = readBarcodeList(productExportCSV);

                
                // MATCH PRICES FROM HASH MAP TO PRODUCT LIST FILE USING SHARED SKU NUMBER
                System.out.println("-- MERGING AND WRITING DISTRUBUTOR PRICES FILE --");
                mergeAndWriteFormTwo(zohoUploadCSV, outputZohoBarcodeFile, productExportBarcodes);
            } 
             catch (IOException e) {
                // Handle IO exception gracefully
                System.err.println("Error occurred while processing uploaded files: " + e.getMessage());
            }
        }

        // EXTRACT 3 DISTINCT PRICES FROM B2B PRICE LIST INTO HASH MAPS
        private static Map<String, String> readPriceList(String priceListFile, String priceType) throws IOException {
            System.out.println("-- READING " + priceType + " PRICES FROM PRICE LIST FILE --");
            String line = "";
            int lineSize = 0;
            Map<String, String> priceList = new HashMap<>();

            try (BufferedReader br = new BufferedReader(new FileReader(priceListFile))) {
                String csvSplitBy = ",";
                int count = 1;
                while ((line = br.readLine()) != null) {                    
                    String[] data = splitCSVLine(line, "prices");
                    lineSize = data.length;
                    if (lineSize == 11) {
                        String sku = data[4]; // Assuming SKU is the 5th column
                        if (sku.length() > 0) {
                            String price;
                            switch(priceType) {
                                case "distributor":
                                    if (data[7] == null || data[7].isEmpty())
                                        price = "0.00";
                                    else
                                        price = data[7];
                                    break;
                                case "jobber":
                                    if (data[8] == null || data[8].isEmpty())
                                        price = "0.00";
                                    else
                                        price = data[8];
                                    break;
                                case "member":
                                    if (data[9] == null || data[9].isEmpty())
                                        price = "0.00";
                                    else
                                        price = data[9];
                                    break;
                                default:
                                    price = "0.00";
                                    break;
                            }
                            priceList.put(sku, price);
                        }
                        count++;
                    }
                    lineSize = 0;
                }
            }
             catch (Exception e) {
                // Handle IO exception gracefully
                System.err.println("Error occurred while processing uploaded files: " + e.getMessage());
            }
            return priceList;
        }

        // EXTRACT 3 DISTINCT PRICES FROM B2B PRICE LIST INTO HASH MAPS
        private static Map<String, String> readBarcodeList(String productListFile) throws IOException {
            System.out.println("-- READING BARCODES FROM PRODUCT LIST FILE --");

            String line = "";
            int lineSize = 0;
            Map<String, String> barcodeList = new HashMap<>();

            try (BufferedReader br = new BufferedReader(new FileReader(productListFile))) {
                String csvSplitBy = ",";
                int count = 1;
                while ((line = br.readLine()) != null) {     

                    String[] data = splitCSVLine(line, "barcodes");
                    lineSize = data.length;

                    String sku = data[14]; // Assuming SKU is the 5th column

                    String barcode = " ";
                    if (sku.length() > 0) {

                        if (data[24] == null || data[24].isEmpty())
                            barcode = " ";
                        else
                            barcode = data[24];

                        barcodeList.put(sku, barcode);
                    }
                    count++;
                }
            }
            catch (Exception e) {
                System.err.println(e.toString());
            }
            return barcodeList;
        }

        private static void mergeAndWriteFormOne(String productFile, String outputFile, Map<String, String> priceList) {
            ArrayList<String> emptySKUs = new ArrayList<>();
            ArrayList<String> notFound = new ArrayList<>();
            
            String outputEmptySKUFile = "empty_sku_from_zoho.csv";
            String outputNotFoundFile = "not_found_sku_in_b2b_price_list.csv";
            
            try (BufferedReader br = new BufferedReader(new FileReader(productFile));
                 BufferedWriter writerDistributorFile = new BufferedWriter(new FileWriter(outputFile));
                 BufferedWriter writerEmptySkuFile = new BufferedWriter(new FileWriter(outputEmptySKUFile));
                 BufferedWriter writerNotFoundFile = new BufferedWriter(new FileWriter(outputNotFoundFile))) {
                
                int count = 1;
                String line;
                String csvSplitBy = ",";
                String newLine;
                while ((line = br.readLine()) != null) {
                    
                    if (count == 1) {
                        newLine =
                                "Item ID," +
                                "Item Name," +
                                "SKU," +
                                "Status," +
                                "is_combo_product," +
                                "Item Price," +
                                "PriceList Rate," +
                                "Discount";
                        writerDistributorFile.write(newLine);
                        writerDistributorFile.newLine();
                    }
                    
                    if (count > 1) {
                        String[] data = line.split(csvSplitBy);
                        String sku = (data.length > 2) ? data[2] : null; // Assuming SKU is the 3rd column
                        
                        // Check if the SKU exists in the distributor prices map
                        if (sku != null && !sku.isEmpty()) {
                            if (priceList.containsKey(sku)) {
                                // Write a new line to the output CSV file with the required format
                                String prices = priceList.get(sku);
                                newLine =
                                        data[0] + "," +
                                        data[1] + "," +
                                        data[2] + "," +
                                        data[3] + "," +
                                        data[4] + "," +
                                        data[5] + "," +
                                        prices;
                                
                                writerDistributorFile.write(newLine);
                                writerDistributorFile.newLine();
                            } else {
                                notFound.add(line);
                            }
                        } else {
                            emptySKUs.add(line);
                        }
                    }
                    count++;
                }
                
                // Write empty SKU lines to the output file
                for (String element : emptySKUs) {
                    writerEmptySkuFile.write(element);
                    writerEmptySkuFile.newLine();
                }
                
                // Write not found SKU lines to the output file
                for (String element : notFound) {
                    writerNotFoundFile.write(element);
                    writerNotFoundFile.newLine();
                }
                
            } catch (IOException e) {
                // Handle IO exception gracefully
                System.err.println("Error occurred while merging and writing: " + e.getMessage());
            }
        }

        private static void mergeAndWriteFormTwo(String zohoFile, String outputFile, Map<String, String> barcodeList) {
            try (BufferedReader reader = new BufferedReader(new FileReader(zohoFile));
                 BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
                
                String line;
                // Read header from input CSV and write it to output CSV
                if ((line = reader.readLine()) != null) {
                    writer.write(line);
                    writer.newLine();
                }
                
                // Process each row in the input CSV file
                while ((line = reader.readLine()) != null) {
                    // Split the row into columns
                    String[] columns = line.split(",");
                    
                    // Extract SKU from a specific column (adjust the index as needed)
                    String sku = (columns.length > 22) ? columns[22] : null; // Assuming SKU is in the 23rd column

                    // Check if the SKU exists in the barcode list and the barcode is not empty
                    if (sku != null && barcodeList.containsKey(sku) && !barcodeList.get(sku).isEmpty()) {
                        // Look up barcode for the SKU in the barcodeList
                        String barcode = barcodeList.get(sku).replace("'", "");
                        // Update the 24th X column UPC
                        columns[23] = barcode;
                        
                        // Write the updated row to the output CSV file
                        writer.write(String.join(",", columns));
                        writer.newLine();
                    }
                }
            } 
            catch (IOException e) {
                // Handle IO exception gracefully
                System.err.println("Error occurred while merging and writing: " + e.getMessage());
            }
        }

        // SPLIT LINE CONSIDERING EMPTY FIELDS
        // HANDLES CONSECUTIVE COMMAS
        // DOES NOT HANDLE COMMAS WITHIN QUOTES
        private static String[] splitCSVLine(String line, String fileType) {
            List<String> parts = new ArrayList<>();
            StringBuilder currentPart = new StringBuilder();
            boolean inQuotes = false;

            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (c == '"') {
                    // Toggle inQuotes flag when encountering double quotes
                    inQuotes = !inQuotes;
                    currentPart.append(c);
                } 
                else if (c == ',' && !inQuotes) {
                    // Split when encountering a comma outside quotes
                    parts.add(currentPart.toString());
                    currentPart.setLength(0);
                } 
                else {
                    // Append other characters to the current part
                    currentPart.append(c);
                }

                // Handle case when reaching end of line
                if (i == line.length() - 1) {
                    parts.add(currentPart.toString());
                }
            }

            if(fileType == "barcodes") {
                // Pad with empty strings to ensure the row size is 56
                while (parts.size() < 56) {
                    parts.add("");
                }
            }

            return parts.toArray(new String[0]);
        }
    }
}