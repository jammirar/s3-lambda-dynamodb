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
            id = row[0]
            Patron_id = row[1]
            Order_date = row[2]
            Order_status = row[3]
            qty = row[4]
            Gross_price = row[5]
            Net_price = row[6]
            Real_Area_name = row[7]
            Seat_number	= row[8]
            Seat_Category_name = row[9]
            Pricing_code = row[10]
            Pricing_Code_name = row[11]
            Item_status = row[12]
            Venue_name = row[13]
            Event_name = row[14]
            Performance_name = row[15]
            Performance_date = row[16]
            Billing_company = row[17]
            Billing_First_name = row[18]
            Billing_Middle_initial = row[19]
            Billing_Last_name = row[20]
            Billing_address_1 = row[21]
            Billing_address_2 = row [22]
            Billing_city = row [23]
            Billing_state = row[24]
            Billing_Zip_code = row[25]
            Billing_country = row[26]
            Billing_phone = row[27]
            Billing_fax = row[28]
            Billing_Cell_phone = row[29]
            Billing_Work_phone = row[30]
            Billing_email = row[31]
            Payment_type = row[32]
            Payment_detail = row[33]
            CC_Last_4_digits = row[34]
            Type_Of_delivery = row[35]
            fees = row[36]
            Local_tax = row[37]
            barcode = row[38]
            department = row[39]
            Scan_status = row[40]
            Date_scanned = row[41]
            User_created = row[42]
            Item_Created_date = row[43]
            Pricing_Code_group = row[44]
            Special_Needs_selection = row[45]
            Forwarded_Email_address = row[46]
            Sales_channel = row[47]
            Agency_name = row[48]
            Agency_Commission_Price_group = row[49]
            Agency_region = row[50]
            Agency_Subregion = row[51]
            Agency_Accounting_code = row[52]
            Agency_Payment_terms = row [53]
            Agency_Primary_First_name = row [54]
            Agency_Primary_Last_name = row[55]
            Agency_email = row[56]
            Agency_address_1 = row[57]
            Agency_address_2 = row[58]
            Agency_city = row[59]
            Agency_country = row[60]
            Agency_state = row[61]
            Agency_Zip_code = row[62]
            Agency_phone = row[63]
            Agent_First_name = row[64]
            Agent_Last_name = row[65]
            Agent_email = row[66]
            Agent_phone = row[67]
            Promo_Discount_amount = row[68]
            Commission_amount = row[69]
            Lead_Guest_First_name = row[70]
            Lead_Guest_Last_name = row[71]
            Lead_Guest_email = row[72]
            Lead_Guest_phone = row[73]
            Order_Promo_code = row[74]
            Uneven_exchange = row[75]
            Item_type = row[76]
            Package_name = row[77]
            Entitlementkeys = row[78]
            OriginalOrderid = row[79]
            Mobile_Ticket_link = row[80]
            portal = row[81]
            comment_1 = row[82]
            comment_10 = row[83]
            comment_11 = row[84]
            comment_12 = row[85]
            comment_13 = row[86]
            comment_14 = row[87]
            comment_15 = row[88]
            comment_16 = row[89]
            comment_17 = row[90]
            comment_18 = row[91]
            comment_19 = row[92]
            comment_2 = row[93]
            comment_20 = row[94]
            comment_21 = row[95]
            comment_22 = row[96]
            comment_23 = row[97]    
            comment_24 = row[98]
            comment_25 = row[99]
            comment_26 = row[100] 
            comment_27 = row[101]
            comment_28 = row[102]
            comment_29 = row[103]
            comment_3 = row[104]
            comment_30 = row[105]
            comment_31 = row[106]
            comment_32 = row[107]
            comment_33 = row[108]
            comment_34 = row[109]
            comment_35 = row[110]
            comment_36 = row[111]
            comment_37 = row[112]
            comment_38 = row[113]
            comment_39 = row[114]
            comment_4 = row[115]
            comment_40 = row[116]
            comment_41 = row[117]
            comment_42 = row[118]
            comment_43 = row[119]
            comment_44 = row[120]
            comment_45 = row[121]
            comment_46 = row[122]
            comment_47 = row[123]
            comment_48 = row[124]
            comment_49 = row[125]
            comment_5 = row[126]
            comment_50 = row[127]
            comment_51 = row[128]
            comment_52 = row[129]
            comment_53 = row[130]
            comment_54 = row[131]
            comment_55 = row[132]
            comment_56 = row[133]
            comment_57 = row[134]
            comment_6 = row[135]
            comment_7 = row[136]
            comment_8 = row[137]
            comment_9 = row[138]
            Voucher_number = row[139]
            Transportation_comment_1 = row[140]

            # Insert record in DynamoDB table
            response = db.put_item(TableName = 'Customers',
                Item = {
                    'Id': {
                        'S': str(id)
                    },
                    'Patron_ID': {
                        'S': str(Patron_id)
                    },
                    'Order_Date': {
                        'S': str(Order_date)
                    },
                    'Order_Status': {
                        'S': str(Order_status)
                    },
                    'Qty': {
                        'S': str(qty)
                    },
                    'Gross_Price': {
                        'S': str(Gross_price)
                    },
                    'Net_Price': {
                        'S': str(Net_price)
                    },
                    'Real_Area_Name': {
                        'S': str(Real_Area_name)
                    },	
                    'Seat_Number': {
                        'S': str(Seat_number)
                    },	
                    'Seat_Category_Name': {
                        'S': str(Seat_Category_name)
                    },
                    'Pricing_Code': {
                        'S': str(Pricing_code)
                    },
                    'Pricing_Code_Name': {
                        'S': str(Pricing_Code_name)
                    },
                    'Item_Status': {
                        'S': str(Item_status)
                    },
                    'Venue_Name': {
                        'S': str(Venue_name)
                    },
                    'Event_Name': {
                        'S': str(Event_name)
                    },
                    'Performance_Name': {
                        'S': str(Performance_name)
                    },
                    'Performance_Date': {
                        'S': str(Performance_date)
                    },
                    'Billing_Company': {
                        'S': str(Billing_company)
                    },
                    'Billing_First_Name': {
                        'S': str(Billing_First_name)
                    },
                    'Billing_Middle_Initial': {
                        'S': str(Billing_Middle_initial)
                    },
                    'Billing_Last_Name': {
                        'S': str(Billing_Last_name)
                    },
                    'Billing_Address_1': {
                        'S': str(Billing_address_1)
                    },
                    'Billing_Address_2': {
                        'S': str(Billing_address_2)
                    },
                    'Billing_City': {
                        'S': str(Billing_city)
                    },
                    'Billing_State': {
                        'S': str(Billing_state)
                    },
                    'Billing_Zip_Code': {
                        'S': str(Billing_Zip_code)
                    },
                    'Billing_Country': {
                        'S': str(Billing_country)
                    },
                    'Billing_Phone': {
                        'S': str(Billing_phone)
                    },
                    'Billing_Fax': {
                        'S': str(Billing_fax)
                    },
                    'Billing_Cell_Phone': {
                        'S': str(Billing_Cell_phone)
                    },
                    'Billing_Work_Phone': {
                        'S': str(Billing_Work_phone)
                    },
                    'Billing_Email': {
                        'S': str(Billing_email)
                    },
                    'Payment_Type': {
                        'S': str(Payment_type)
                    },
                    'Payment_Detail': {
                        'S': str(Payment_detail)
                    },
                    'CC_Last_4_Digits': {
                        'S': str(CC_Last_4_digits)
                    },
                    'Type_Of_Delivery': {
                        'S': str(Type_Of_delivery)
                    },
                    'Fees': {
                        'S': str(fees)
                    },
                    'Local_Tax': {
                        'S': str(Local_tax)
                    },
                    'Barcode': {
                        'S': str(barcode)
                    },
                    'Department': {
                        'S': str(department)
                    },
                    'Scan_Status': {
                        'S': str(Scan_status)
                    },
                    'Date_Scanned': {
                        'S': str(Date_scanned)
                    },
                    'User_Created': {
                        'S': str(User_created)
                    },
                    'Item_Created_Date': {
                        'S': str(Item_Created_date)
                    },
                    'Pricing_Code_Group': {
                        'S': str(Pricing_Code_group)
                    },
                    'Special_Needs_Selection': {
                        'S': str(Special_Needs_selection)
                    },
                    'Forwarded_Email_Address': {
                        'S': str(Forwarded_Email_address)
                    },
                    'Sales_Channel': {
                        'S': str(Sales_channel)
                    },
                    'Agency_Name': {
                        'S': str(Agency_name)
                    },
                    'Agency_Commission_Price_Group': {
                        'S': str(Agency_Commission_Price_group)
                    },
                    'Agency_Region': {
                        'S': str(Agency_region)
                    },
                    'Agency_SubRegion': {
                        'S': str(Agency_Subregion)
                    },
                    'Agency_Accounting_Code': {
                        'S': str(Agency_Accounting_code)
                    },
                    'Agency_Payment_Terms': {
                        'S': str(Agency_Payment_terms)
                    },
                    'Agency_Primary_First_Name': {
                        'S': str(Agency_Primary_First_name)
                    },
                    'Agency_Email': {
                        'S': str(Agency_email)
                    },
                    'Agency_Address_1': {
                        'S': str(Agency_address_1)
                    },
                    'Agency_address_2': {
                        'S': str(Agency_address_2)
                    },
                    'Agency_City': {
                        'S': str(Agency_city)
                    },
                    'Agency_Country': {
                        'S': str(Agency_country)
                    },
                    'Agency_State': {
                        'S': str(Agency_state)
                    },
                    'Agency_Zip_Code': {
                        'S': str(Agency_Zip_code)
                    },
                    'Agency_Phone': {
                        'S': str(Agency_phone)
                    },
                    'Agent_First_Name': {
                        'S': str(Agent_First_name)
                    },
                    'Agent_Last_Name': {
                        'S': str(Agent_Last_name)
                    },
                    'Agent_Email': {
                        'S': str(Agent_email)
                    },
                    'Agent_Phone': {
                        'S': str(Agent_phone)
                    },
                    'Promo_Discount_Amount': {
                        'S': str(Promo_Discount_amount)
                    },
                    'Commission_Amount': {
                        'S': str(Commission_amount)
                    },
                    'Lead_Guest_First_Name': {
                        'S': str(Lead_Guest_First_name)
                    },
                    'Lead_Guest_Last_Name': {
                        'S': str(Lead_Guest_Last_name)
                    },
                    'Lead_Guest_Email': {
                        'S': str(Lead_Guest_email)
                    },
                    'Lead_Guest_Phone': {
                        'S': str(Lead_Guest_phone)
                    },
                    'Order_Promo_Code': {
                        'S': str(Order_Promo_code)
                    },
                    'Uneven_Exchange': {
                        'S': str(Uneven_exchange)
                    },
                    'Item_Type': {
                        'S': str(Item_type)
                    },
                    'Package_Name': {
                        'S': str(Package_name)
                    },
                    'EntitlementKeys': {
                        'S': str(Entitlementkeys)
                    },
                    'OriginalOrderId': {
                        'S': str(OriginalOrderid)
                    },
                    'Mobile_Ticket_Link': {
                        'S': str(Mobile_Ticket_link)
                    },
                    'Portal': {
                        'S': str(portal)
                    },
                    'Comment_1': {
                        'S': str(comment_1)
                    },
                    'Comment_10': {
                        'S': str(comment_10)
                    },
                    'Comment_11': {
                        'S': str(comment_11)
                    },
                    'Comment_12': {
                        'S': str(comment_12)
                    },
                    'Comment_13': {
                        'S': str(comment_13)
                    },
                    'Comment_14': {
                        'S': str(comment_14)
                    },
                    'Comment_15': {
                        'S': str(comment_15)
                    },
                    'Comment_16': {
                        'S': str(comment_16)
                    },
                    'Comment_17': {
                        'S': str(comment_17)
                    },
                    'Comment_18': {
                        'S': str(comment_18)
                    },
                    'Comment_19': {
                        'S': str(comment_19)
                    },
                    'Comment_2': {
                        'S': str(comment_2)
                    },
                    'Comment_20': {
                        'S': str(comment_20)
                    },
                    'Comment_21': {
                        'S': str(comment_21)
                    },
                    'Comment_22': {
                        'S': str(comment_22)
                    },
                    'Comment_23': {
                        'S': str(comment_23)
                    },
                    'Comment_24': {
                        'S': str(comment_24)
                    },
                    'Comment_25': {
                        'S': str(comment_25)
                    },
                    'Comment_26': {
                        'S': str(comment_26)
                    },
                    'Comment_27': {
                        'S': str(comment_27)
                    },
                    'Comment_28': {
                        'S': str(comment_28)
                    },
                    'Comment_29': {
                        'S': str(comment_29)
                    },
                    'Comment_3': {
                        'S': str(comment_3)
                    },
                    'Comment_30': {
                        'S': str(comment_30)
                    },
                    'Comment_31': {
                        'S': str(comment_31)
                    },
                    'Comment_32': {
                        'S': str(comment_32)
                    },
                    'Comment_33': {
                        'S': str(comment_33)
                    },
                    'Comment_34': {
                        'S': str(comment_34)
                    },
                    'Comment_35': {
                        'S': str(comment_35)
                    },
                    'Comment_36': {
                        'S': str(comment_36)
                    },
                    'Comment_37': {
                        'S': str(comment_37)
                    },
                    'Comment_38': {
                        'S': str(comment_38)
                    },
                    'Comment_39': {
                        'S': str(comment_39)
                    },
                    'Comment_4': {
                        'S': str(comment_4)
                    },
                    'Comment_40': {
                        'S': str(comment_40)
                    },
                    'Comment_41': {
                        'S': str(comment_41)
                    },
                    'Comment_42': {
                        'S': str(comment_42)
                    },
                    'Comment_43': {
                        'S': str(comment_43)
                    },
                    'Comment_44': {
                        'S': str(comment_44)
                    },
                    'Comment_45': {
                        'S': str(comment_45)
                    },
                    'Comment_46': {
                        'S': str(comment_46)
                    },
                    'Comment_47': {
                        'S': str(comment_47)
                    },
                    'Comment_48': {
                        'S': str(comment_48)
                    },
                    'Comment_49': {
                        'S': str(comment_49)
                    },
                    'Comment_5': {
                        'S': str(comment_5)
                    },
                    'Comment_50': {
                        'S': str(comment_50)
                    },
                    'Comment_51': {
                        'S': str(comment_51)
                    },
                    'Comment_52': {
                        'S': str(comment_52)
                    },
                    'Comment_53': {
                        'S': str(comment_53)
                    },
                    'Comment_54': {
                        'S': str(comment_54)
                    },
                    'Comment_55': {
                        'S': str(comment_55)
                    },
                    'Comment_56': {
                        'S': str(comment_56)
                    },
                    'Comment_57': {
                        'S': str(comment_57)
                    },
                    'Comment_6': {
                        'S': str(comment_6)
                    },
                    'Comment_7': {
                        'S': str(comment_7)
                    },
                    'Comment_8': {
                        'S': str(comment_8)
                    },
                    'Comment_9': {
                        'S': str(comment_9)
                    },
                    'Voucher_Number': {
                        'S': str(Voucher_number)
                    },
                    'Transportation_Comment_1': {
                        'S': str(Transportation_comment_1)
                    },
                    
                })
            
        print('Successfully added ', rows, ' in Customers Table')        
        
    except Exception as e:
        print(str(e))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Function execution completed!')
    }
