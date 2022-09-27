# s3-lambda-dynamodb
upload to s3 a .csv file and lambda to dynamodb 


Prerequisites:

Clone repo:
$ git clone https://github.com/srs2210/aws-lambda-csv-2-dynamodb.git

$ cd aws-lambda-csv-2-dynamodb
 
 Tips: 
       - Remember that S3 bucket name must be unique across all AWS.
       - Region defined in the files: region =  "us-east-1"
       
       
 1. Existing S3 bucket with name: bucket = "${var.function_name}-bucket007-20220913". defined in file lambda.tf line # 39 must exist before running terraform plan/apply.   (csv-2-dynamodb-bucket007-20220913)

Deploy infrastructure:
  1. terraform init
  2. terraform plan
  3. terraform apply -auto-approve


Testing 
  Run the following command to upload the sample csv file to s3:
  1. aws s3 cp FILENAME_WITH_DUMMY_DATA.csv s3://csv-2-dynamodb-bucket007-20220913/
  Run the following command to read the entries from DynamoDB
  2. aws dynamodb scan --table-name Customers
  or thru AWS DynamoDB dashboard --> Explore items --> Scan

Deleting resources
  Make sure that the bucket it's empty
  1. aws s3 rm s3://csv-2-dynamodb-bucket007-20220913 --recursive
  Delete all existing resources
  2. terraform destroy -auto-approve
