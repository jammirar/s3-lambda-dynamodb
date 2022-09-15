import json, boto3, csv

region = 'us-east-1'

def lambda_handler(event, context):
    
    try:
        s3 = boto3.client('s3')        
        db = boto3.client('dynamodb', region_name = region)

        # Get the uploaded file details
        bucketName = event['Records'][0]['s3']['bucket']['name'] 

        fileName = event['Records'][0]['s3']['object']['key']  

        print('Uploaded file name: ', fileName)
        
        # Fetch the data from uploaded file        
        csvFile = s3.get_object(Bucket = bucketName, Key = fileName)  

        dataSet = csvFile['Body'].read().decode('utf-8').split('\n')  

        csvReader = csv.reader(dataSet, delimiter = ',', quotechar = '"')
        
        rows = 0
        
        for row in csvReader:
            rows += 1
            Order_ID = row[0]
            Patron_ID = row[1]
            Order_Date = row[2]
            Order_Status = row[3]
	    Qty = row[4]
            Gross_Price = row[5]
	    Net_Price = row[6]
            Real_Area_Name = row[7]
            Seat_Number	= row[8]
	    Seat_Category_Name = row[9]

            # Insert record in DynamoDB table
            response = db.put_item(TableName = 'Customers',
                Item = {
                    'Order_ID': {
                        'N': str(Order_ID)
                    },
                    'Patron_ID': {
                        'N': str(Patron_ID)
                    },
                    'Order_Date': {
                        'S': str(Order_Date)
                    },
                    'Order_Status': {
                        'S': str(Order_status)
                    },
                    'Qty': {
                        'N': str(Qty)
                    },
		    'Gross_Price': {
                        'N': str(Gross_Price)
                    },
	            'Net_Price': {
                        'N': str(Net_Price)
                    },
		    'Real_Area_Name': {
                        'S': str(Real_Area_Name)
                    },	
		    'Seat_Number': {
                        'N': str(Seat_Number)
                    },	
		    'Seat_Category_Name': {
                        'S': str(Seat_Category_Name)
                    },
                })
            
        print('Successfully added ', rows, ' in Customers Table')        
        
    except Exception as e:
        print(str(e))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Function execution completed!')
    }
