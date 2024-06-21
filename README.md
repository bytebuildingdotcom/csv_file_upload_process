The purpose of this program is to allow a user to upload csv files that will create other csv files based on the input of the data.

There are two distinct forms that allow for different logic and functionality.

The input files are predfined and output files are tailored to what is expected.

The program is built to run as a Windows service.

Complile and Run
  
	javac FileUploadServer.java
	java FileUploadServer

Package JAR

	jar cfm FileUploadServer.jar FileUploadServer.mf FileUploadServer.class FileUploadServer\$ClientHandler.class
	jar tf FileUploadServer.jar
