import os, base64, pickle, tempfile, sys
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from bs4 import BeautifulSoup
import time
import re
from ollama import chat
from pydantic import BaseModel
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

SCOPES = ['https://www.googleapis.com/auth/gmail.modify',
          'https://www.googleapis.com/auth/drive',
          'https://www.googleapis.com/auth/spreadsheets']
LABEL           = 'GCS/Weekly Timesheet'
TOKEN_FILE      = 'token.pickle'
CREDENTIALS_FILE= 'credentials.json'      
CHROME_BIN = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"

def get_gmail_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow  = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(creds, token)

    return build('gmail', 'v1', credentials=creds)

def get_sheets_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            creds = pickle.load(token)
            
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow  = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            
    return build('sheets', 'v4', credentials=creds)

def get_drive_service():
    creds = None
    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow  = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

    return build('drive', 'v3', credentials=creds)

gmail_service = get_gmail_service()
sheets_service = get_sheets_service()
drive_service = get_drive_service()
  
def latest_summary_threads(max_threads=2):
    lbls = gmail_service.users().labels().list(userId='me').execute()['labels']
    label_id = next((l['id'] for l in lbls if l['name'] == LABEL), None)
    if not label_id:
        print(f"[!] Gmail label \"{LABEL}\" not found"); return []

    threads = gmail_service.users().threads().list(
                userId='me', labelIds=[label_id], maxResults=max_threads
              ).execute().get('threads', [])
    return [t['id'] for t in threads]

def get_thread_html(thread_id):
    """Return the first HTML part of the first message in the thread."""
    thread = gmail_service.users().threads().get(
               userId='me', id=thread_id, format='full'
             ).execute()
    msg = thread['messages'][0]['payload']
    # multipart: look for a part with mimeType == text/html
    if 'parts' in msg:
        for p in msg['parts']:
            if p.get('mimeType') == 'text/html' and 'data' in p['body']:
                return base64.urlsafe_b64decode(p['body']['data']).decode()
    # single-part fallback
    if msg.get('mimeType') == 'text/html' and 'data' in msg['body']:
        return base64.urlsafe_b64decode(msg['body']['data']).decode()
    return None

def get_thread_subject(thread_id):
    """Get the subject of the first message in the thread."""
    thread = gmail_service.users().threads().get(
               userId='me', id=thread_id, format='metadata'
             ).execute()
    
    # Find subject in headers
    headers = thread['messages'][0]['payload']['headers']
    subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), None)
    
    # Clean up subject for filename
    if subject:
        # Replace invalid filename characters with underscores
        clean_subject = re.sub(r'[\\/*?:"<>|]', '_', subject)
        # Limit length to avoid too long filenames
        clean_subject = clean_subject[:100]
        return clean_subject
    
    return f"timesheet_{thread_id}"  # Fallback if no subject found

def extract_header_and_body(full_html: str) -> str:
    """
    Return a stand-alone HTML string containing everything from
    <!-- == Header Section == --> … <!-- == //Footer Section == -->
    including the <style> blocks from the original.
    """
    start_marker = '<!-- == Header Section == -->'
    end_marker   = '<!-- == //Footer Section == -->'

    start_idx = full_html.find(start_marker)
    end_idx   = full_html.find(end_marker)
    if start_idx == -1 or end_idx == -1:
        raise ValueError("Couldn't find header/body section markers in the HTML.")

    # grab the section *between* the two markers
    section_html = full_html[start_idx : end_idx + len(end_marker)]

    # pull in any global <style> blocks so your CSS still works
    soup = BeautifulSoup(full_html, 'html.parser')
    style_blocks = ''.join(str(tag) for tag in soup.find_all('style'))

    style_blocks += """
      <style>
         html, body { overflow: hidden !important; }
         ::-webkit-scrollbar { display: none; }
      </style>
    """

    # wrap it all up in a minimal page
    return f"""<!DOCTYPE html>
<html>
  <head>
    {style_blocks}
  </head>
  <body>
    {section_html}
  </body>
</html>"""

def html_to_png(html, folder_name, filename):
    try:
        # Extract only the relevant section
        table_only = extract_header_and_body(html)        

        # Create a temporary HTML file
        with tempfile.NamedTemporaryFile(suffix='.html', delete=False) as temp:
            temp_path = temp.name
            temp.write(table_only.encode('utf-8'))
            print(f"[i] Created temp HTML file: {temp_path}")
        
        result_path = os.path.join(folder_name, filename)
        
        # Take screenshot with Selenium
        try:
            print(f"[i] Attempting screenshot with Selenium")
            chrome_options = Options()
            chrome_options.add_argument("--headless=new")
            chrome_options.add_argument("--window-size=700,1100")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.binary_location = CHROME_BIN
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=chrome_options)
            
            file_url = f"file:///{temp_path.replace('\\', '/')}"
            print(f"[i] Loading URL: {file_url}")
            driver.get(file_url)
            
            # Wait for page to load
            time.sleep(2)
            
            # Take screenshot
            print(f"[i] Taking screenshot and saving to: {result_path}")
            driver.save_screenshot(result_path)
            driver.quit()
            
            # Check if screenshot was created
            if os.path.exists(result_path):
                print(f"[i] Selenium successfully created screenshot at: {result_path}")
            else:
                print(f"[!] Selenium could not create screenshot. File doesn't exist after save_screenshot call.")
                raise RuntimeError("Failed to create screenshot with Selenium")
        except Exception as e:
            print(f"[!] Selenium error: {str(e)}")
            import traceback
            print(traceback.format_exc())
            raise
        
        # Clean up the temp file
        os.unlink(temp_path)
        print(f"[i] Removed temp file: {temp_path}")
        
        return result_path
    except Exception as e:
        print(f"[!] Detailed error in html_to_png: {str(e)}")
        print(f"[!] Error type: {type(e).__name__}")
        import traceback
        print(traceback.format_exc())
        raise
    
def duplicate_invoice_tab(spreadsheet_id):
    # 1) read all sheets
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_list = meta.get('sheets', [])

    # 2) pick the highest invoice no from tab titles
    def extract_invoice_number(title):
        # Extract just the number part after # and before any parentheses
        number_part = title.split('#')[-1].strip()
        # Handle case like "10 (1)" by taking only the part before any space or parenthesis
        number_part = number_part.split()[0].split('(')[0].strip()
        try:
            return int(number_part)
        except ValueError:
            return 0
    
    # Ensure there is at least one sheet with "Invoice #" in the title
    invoice_sheets = [s for s in sheet_list if s['properties']['title'].startswith("Invoice #")]
    if not invoice_sheets:
        raise ValueError("No sheets with 'Invoice #' found.")

    latest = max(invoice_sheets, key=lambda sh: extract_invoice_number(sh['properties']['title']))
    old_id   = latest['properties']['sheetId']
    old_title= latest['properties']['title']
    next_num = extract_invoice_number(old_title) + 1
    new_title= f"Invoice #{next_num}"

    # 3) duplicate via batchUpdate
    body = {
      'requests': [{
        'duplicateSheet': {
          'sourceSheetId': old_id,
          'insertSheetIndex': len(sheet_list),  # Ensure the new sheet is added at the end
          'newSheetName': new_title
        }
      }]
    }
    response = sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body).execute()

    # Check if the duplication was successful
    if 'replies' in response and response['replies']:
        print(f"Duplicated sheet '{old_title}' as '{new_title}'")
    else:
        raise RuntimeError("Failed to duplicate the sheet.")

    return new_title

def set_sheet_data(spreadsheet_id, sheet_name, data):
    # Get the sheet ID
    sheet_id = None
    for sheet in sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute().get('sheets', []):
        if sheet['properties']['title'] == sheet_name:
            sheet_id = sheet['properties']['sheetId']
            break
    
    if not sheet_id:
        raise ValueError(f"Sheet '{sheet_name}' not found in spreadsheet '{spreadsheet_id}'")
    
    # Process the data updates
    # Expected data format: {
    #   'invoice_no': str,
    #   'submission_date': str,  # format: MM/DD/YYYY
    #   'week_one_date': str,
    #   'week_two_date': str,
    #   'week_one_hours': int,
    #   'week_two_hours': int
    # }
    
  
    # Update values using batchUpdate with valueInputOption=USER_ENTERED
    value_ranges = []
    
    # F12:G12 -> #[invoice_no]
    if 'invoice_no' in data:
        value_ranges.append({
            'range': f"'{sheet_name}'!F12:G12",
            'values': [[f"#{data['invoice_no']}"]]
        })
    
    # B9:C9 -> Submitted on [MM/DD/YYYY] (GMT+8)
    if 'submission_date' in data:
        value_ranges.append({
            'range': f"'{sheet_name}'!B9:C9",
            'values': [[f"Submitted on {data['submission_date']} (GMT+8)"]]
        })
    
    # B19:D19 -> [week_one_date]
    if 'week_one_date' in data:
        value_ranges.append({
            'range': f"'{sheet_name}'!B19:D19",
            'values': [[data['week_one_date']]]
        })
    
    # B20:D20 -> [week_two_date]
    if 'week_two_date' in data:
        value_ranges.append({
            'range': f"'{sheet_name}'!B20:D20",
            'values': [[data['week_two_date']]]
        })

    # E19 -> [week_one_hours]
    if 'week_one_hours' in data:
        value_ranges.append({
            'range': f"'{sheet_name}'!E19",
            'values': [[data['week_one_hours']]]
        })

    # E20 -> [week_two_hours]
    if 'week_two_hours' in data:
        value_ranges.append({
            'range': f"'{sheet_name}'!E20",
            'values': [[data['week_two_hours']]]
        })
    
    # Execute the batch update if there are any value ranges to update
    if value_ranges:
        body = {
            'valueInputOption': 'USER_ENTERED',
            'data': value_ranges
        }
        sheets_service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body=body
        ).execute()
        
    return sheet_id

def get_total_hours(image_path):
    class TotalHours(BaseModel):
      total_hours: float
    
    response = chat(
      messages=[
        {
          'role': 'user',
          'content': 'Please extract the exact total hours from the image. Do not round, keep any decimal places.',
          'images': [image_path]
        }
      ],
      model='llama3.2-vision',
      format=TotalHours.model_json_schema(),
    )
    
    total_hours = TotalHours.model_validate_json(response.message.content)
    return total_hours.total_hours

def save_pdf_to_sheet(spreadsheet_id, sheet_name=None, filename=None):
    # Get all sheets
    sheets_metadata = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    sheet_list = sheets_metadata.get('sheets', [])
    
    # If no sheet name provided, use the last sheet
    if sheet_name is None:
        last_sheet = sheet_list[-1]
        sheet_name = last_sheet['properties']['title']
        sheet_id = last_sheet['properties']['sheetId']
        print(f"Using last sheet: {sheet_name}")
    else:
        # Find the specified sheet
        sheet_id = None
        for sheet in sheet_list:
            if sheet['properties']['title'] == sheet_name:
                sheet_id = sheet['properties']['sheetId']
                break
        
        if not sheet_id:
            raise ValueError(f"Sheet '{sheet_name}' not found in spreadsheet '{spreadsheet_id}'")
    
    # If no filename provided, use sheet name
    if filename is None:
        filename = f"{sheet_name}.pdf"
    
    # Export the PDF using the correct approach for a single sheet
    # Using the sheets.spreadsheets.export endpoint with gid parameter
    # Construct the export URL manually to ensure only the specific sheet is exported    

    # Build the export URL
    export_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/export"
    params = {
        'format': 'pdf',
        'gid': sheet_id,
        'size': 'letter',
        'portrait': 'true',
        'fitw': 'true',         # Fit width
        'gridlines': 'false',   # Hide gridlines
        'printtitle': 'false',  # Hide document title
        'sheetnames': 'false',  # Hide sheet names
        'pagenum': 'false',     # Hide page numbers
        'attachment': 'true'    # Download as attachment
    }
    
    # Convert params to URL query string
    query_params = '&'.join([f"{k}={v}" for k, v in params.items()])
    export_url = f"{export_url}?{query_params}"
    
    # Use the built-in HTTP client from drive service
    http = drive_service._http
    
    # Make the request
    response, content = http.request(export_url)
    
    # Check if the request was successful
    if response.status != 200:
        raise Exception(f"Error exporting PDF: {response.status} {response.reason}")
    
    # Save the content to a file
    with open(filename, 'wb') as f:
        f.write(content)
    
    print(f"PDF saved successfully: {filename}")
    return filename

def create_email_draft(invoice_no, date_range, pdf_path, timesheet_images):
    """Create an email draft with invoice PDF and timesheet screenshots as attachments."""
    
    # Email details
    from_email = "carnaje.michaeljames@gmail.com"
    to_email = "elsie@gcaresolution.com"
    cc_email = "dianne@gcaresolution.com"
    subject = f"Salary Invoice #{invoice_no}, {date_range}"
    
    # Email body
    body = """Good day po!

Please find the attached files for your reference.

Thank you!

Best regards,
Mj Carnaje"""
    
    # Create message
    message = MIMEMultipart()
    message['From'] = f"Michael James <{from_email}>"
    message['To'] = to_email
    message['Cc'] = cc_email
    message['Subject'] = subject
    
    # Add body to email
    message.attach(MIMEText(body, 'plain'))
    
    # Attach PDF
    if os.path.exists(pdf_path):
        with open(pdf_path, "rb") as attachment:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(attachment.read())
        
        encoders.encode_base64(part)
        part.add_header(
            'Content-Disposition',
            f'attachment; filename= {os.path.basename(pdf_path)}'
        )
        message.attach(part)
        print(f"Attached PDF: {os.path.basename(pdf_path)}")
    
    # Attach timesheet images (limit to 2 most recent)
    image_list = list(timesheet_images.values())[-2:]  # Get last 2 images
    for image_path in image_list:
        if os.path.exists(image_path):
            with open(image_path, "rb") as attachment:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(attachment.read())
            
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename= {os.path.basename(image_path)}'
            )
            message.attach(part)
            print(f"Attached timesheet: {os.path.basename(image_path)}")
    
    # Convert to base64 encoded string
    raw_message = base64.urlsafe_b64encode(message.as_bytes()).decode()
    
    # Create draft
    draft_body = {
        'message': {
            'raw': raw_message
        }
    }
    
    try:
        draft = gmail_service.users().drafts().create(
            userId='me',
            body=draft_body
        ).execute()
        
        draft_id = draft['id']
        print(f"Email draft created successfully! Draft ID: {draft_id}")
        print(f"Subject: {subject}")
        print(f"To: {to_email}")
        print(f"CC: {cc_email}")
        return draft_id
        
    except Exception as e:
        print(f"Error creating email draft: {str(e)}")
        return None

def main():
    # --skip-screenshot
    args = sys.argv[1:]
    skip_screenshot = "--skip-screenshot" in args
    
    # date -> (subject, html)
    email_map = []
    
    for tid in latest_summary_threads():
        html = get_thread_html(tid)
        
        if not html:
            print(f"[!] No HTML part found in thread {tid}"); continue
            
        # Get the subject for the filename
        subject = get_thread_subject(tid)
        date = subject.replace("Weekly timesheet summary for ", "")

        email_map.append((date, (subject, html)))
    
    # Ensure there are at least two weeks to compare
    if len(email_map) < 2:
        raise ValueError("Not enough data to determine folder name. At least two weeks are required.")
    
    # Duplicate invoice tab
    spreadsheet_id = "1ejsCfqnt_2-taD_uyTBnjF5u92sBd_RWoSmRkjgkTnE"
    new_invoice_title = duplicate_invoice_tab(spreadsheet_id)
    print(f"Duplicated invoice tab: {new_invoice_title}")
    
    # Extract invoice number from the title - this will be our folder name
    invoice_no = new_invoice_title.split('#')[1].strip() if '#' in new_invoice_title else ""

    base_folder = "invoices"

    os.makedirs(base_folder, exist_ok=True)
    
    folder_name = f"{base_folder}/{new_invoice_title}"
    
    # Create folder
    print(f"Creating folder: {folder_name}")
    os.makedirs(folder_name, exist_ok=True)
    
    # Helper functions for date parsing
    def month_to_num(month_abbr):
        months = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
        }
        return months.get(month_abbr, 0)
    
    def parse_date(date_str):
        parts = date_str.split()
        if len(parts) >= 2:
            month, day = parts[0], parts[1]
            try:
                return (month_to_num(month), int(day))
            except (ValueError, KeyError):
                return (0, 0)
        return (0, 0)
    
    # Extract week dates from email subjects
    week_dates = []
    for date, (subject, _) in email_map:
        if "Weekly timesheet summary for " in subject:
            week_range = subject.replace("Weekly timesheet summary for ", "")
            week_dates.append(week_range)
    
    # Sort week dates
    week_dates.sort(key=lambda date_range: parse_date(date_range.split(" - ")[0]))
    
    # Calculate overall date range for email subject
    overall_date_range = ""
    if len(week_dates) >= 2:
        # Get start date from first week and end date from last week
        first_week = week_dates[0]
        last_week = week_dates[-1]
        
        # Extract start date from first week (e.g., "May 30" from "May 30 - Jun 5")
        first_start = first_week.split(" - ")[0]
        
        # Extract end date from last week properly
        last_week_parts = last_week.split(" - ")
        last_start = last_week_parts[0]
        last_end = last_week_parts[1]
        
        # If end date doesn't contain month, use month from start of that week
        if " " not in last_end:  # Just a day number (e.g., "12")
            last_start_parts = last_start.split(" ")
            if len(last_start_parts) >= 1:
                last_month = last_start_parts[0]  # Get month from start of last week
                last_end = f"{last_month} {last_end}"
        
        overall_date_range = f"{first_start} - {last_end}"
        print(f"Overall date range for email: {overall_date_range}")
    elif len(week_dates) == 1:
        overall_date_range = week_dates[0]
    
    # Calculate submission date (2 days after the last day of latest timesheet)
    from datetime import datetime, timedelta
    
    submission_date = None
    if week_dates:
        latest_week_range = week_dates[-1]  # Get the latest week range
        
        # Extract the end date (e.g., from "Apr 11 - 17" get "17")
        range_parts = latest_week_range.split(" - ")
        if len(range_parts) == 2:
            start_part, end_part = range_parts
            
            # Handle case where end date might include month (e.g., "Apr 25 - May 1")
            if " " in end_part:  # End has month name (e.g., "May 1")
                end_month, end_day = end_part.split(" ", 1)
                end_month_num = month_to_num(end_month)
                end_day_num = int(end_day)
            else:  # End is just a day number (e.g., "17")
                # Use month from start date
                start_parts = start_part.split(" ")
                end_month = start_parts[0]  # Get month from start date (e.g., "Apr")
                end_month_num = month_to_num(end_month)
                end_day_num = int(end_part)
            
            # Get current year
            current_year = datetime.now().year
            
            # Create datetime object for end date
            end_date = datetime(current_year, end_month_num, end_day_num)
            
            # Add 2 days for submission date
            submission_date = end_date + timedelta(days=2)
            submission_date_str = submission_date.strftime("%m/%d/%Y")
            print(f"Calculated submission date: {submission_date_str} (2 days after {end_month} {end_day_num})")
    
    # If submission date couldn't be calculated, fall back to current date
    if not submission_date:
        submission_date = datetime.now()
        submission_date_str = submission_date.strftime("%m/%d/%Y")
        print(f"Using current date for submission: {submission_date_str}")
    
    # Save timesheet screenshots in the folder
    timesheet_images = {}
    for date, (subject, html) in email_map:
        filename = f"{date}.png"
        if not skip_screenshot:
            image_path = html_to_png(html, folder_name, filename)
            timesheet_images[date] = image_path
    
    # Extract hours from timesheet images
    week_hours = {}
    if not skip_screenshot and timesheet_images:
        print("Extracting hours from timesheet images...")
        for date, image_path in timesheet_images.items():
            try:
                hours = get_total_hours(image_path)
                week_hours[date] = hours
                print(f"Extracted {hours} hours from {date}")
            except Exception as e:
                print(f"Error extracting hours from {date}: {str(e)}")
                week_hours[date] = 40.0  # fallback to 40.0 hours
    
    # Map hours to weeks in chronological order
    sorted_weeks = sorted(week_hours.keys(), key=lambda date_range: parse_date(date_range.split(" - ")[0]))
    week_one_hours = week_hours.get(sorted_weeks[0], 40.0) if sorted_weeks else 40.0
    week_two_hours = week_hours.get(sorted_weeks[1], 40.0) if len(sorted_weeks) > 1 else 40.0
    
    # Set sheet data with extracted information
    sheet_data = {
        'invoice_no': invoice_no,
        'submission_date': submission_date_str,
        'week_one_hours': week_one_hours,
        'week_two_hours': week_two_hours,
    }
    
    # Add week dates if available
    if len(week_dates) >= 2:
        sheet_data['week_one_date'] = week_dates[0]
        sheet_data['week_two_date'] = week_dates[1]
    elif len(week_dates) == 1:
        sheet_data['week_one_date'] = week_dates[0]
    
    # Update the sheet with the data
    set_sheet_data(spreadsheet_id, new_invoice_title, sheet_data)
    print(f"Updated invoice {invoice_no} with dates: {', '.join(week_dates)} and hours: {week_one_hours}, {week_two_hours}")

    # Save the PDF to the sheet (using same name as folder)
    pdf_filename = f"{new_invoice_title}.pdf"
    pdf_path = os.path.join(folder_name, pdf_filename)
    save_pdf_to_sheet(spreadsheet_id, new_invoice_title, pdf_path)
    print(f"PDF saved to: {pdf_path}")
    
    # Create email draft with attachments
    if overall_date_range and not skip_screenshot:
        print("\nCreating email draft...")
        draft_id = create_email_draft(
            invoice_no=invoice_no,
            date_range=overall_date_range,
            pdf_path=pdf_path,
            timesheet_images=timesheet_images
        )
        if draft_id:
            print(f"Email draft created successfully with ID: {draft_id}")
        else:
            print("Failed to create email draft")
    else:
        print("Skipping email draft creation (missing date range or screenshots skipped)")

def create_draft_for_latest_invoice():
    invoices_folder = "invoices"
    
    # Check if invoices folder exists
    if not os.path.exists(invoices_folder):
        print(f"Invoices folder '{invoices_folder}' not found!")
        return
    
    # Get all invoice folders
    invoices = [f for f in os.listdir(invoices_folder) if os.path.isdir(os.path.join(invoices_folder, f))]
    
    if not invoices:
        print("No invoice folders found!")
        return
    
    # Find the latest invoice folder by creation time
    latest_invoice = max(invoices, key=lambda x: os.path.getctime(os.path.join(invoices_folder, x)))
    latest_invoice_path = os.path.join(invoices_folder, latest_invoice)
    print(f"Latest invoice: {latest_invoice}")
    print(f"Latest invoice path: {latest_invoice_path}")
    
    # Extract invoice number from folder name (e.g., "Invoice #33" -> "33")
    invoice_no = ""
    if "#" in latest_invoice:
        invoice_no = latest_invoice.split('#')[1].strip()
    else:
        print("Could not extract invoice number from folder name")
        return
    
    # Find PDF file in the folder
    pdf_files = [f for f in os.listdir(latest_invoice_path) if f.endswith('.pdf')]
    if not pdf_files:
        print("No PDF file found in the invoice folder!")
        return
    
    pdf_path = os.path.join(latest_invoice_path, pdf_files[0])
    print(f"Found PDF: {pdf_files[0]}")
    
    # Find timesheet images (PNG files)
    png_files = [f for f in os.listdir(latest_invoice_path) if f.endswith('.png')]
    timesheet_images = {}
    
    for png_file in png_files:
        # Remove .png extension to get date range
        date_range = png_file[:-4]  # Remove .png
        full_path = os.path.join(latest_invoice_path, png_file)
        timesheet_images[date_range] = full_path
        print(f"Found timesheet: {png_file}")
    
    # Calculate overall date range for email subject
    if not timesheet_images:
        print("No timesheet images found!")
        return
    
    # Helper functions for date parsing (same as in main())
    def month_to_num(month_abbr):
        months = {
            'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
            'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
        }
        return months.get(month_abbr, 0)
    
    def parse_date(date_str):
        parts = date_str.split()
        if len(parts) >= 2:
            month, day = parts[0], parts[1]
            try:
                return (month_to_num(month), int(day))
            except (ValueError, KeyError):
                return (0, 0)
        return (0, 0)
    
    # Sort date ranges chronologically
    week_dates = list(timesheet_images.keys())
    week_dates.sort(key=lambda date_range: parse_date(date_range.split(" - ")[0]))
    
    # Calculate overall date range
    overall_date_range = ""
    if len(week_dates) >= 2:
        # Get start date from first week and end date from last week
        first_week = week_dates[0]
        last_week = week_dates[-1]
        
        # Extract start date from first week (e.g., "May 30" from "May 30 - Jun 5")
        first_start = first_week.split(" - ")[0]
        
        # Extract end date from last week properly
        last_week_parts = last_week.split(" - ")
        last_start = last_week_parts[0]
        last_end = last_week_parts[1]
        
        # If end date doesn't contain month, use month from start of that week
        if " " not in last_end:  # Just a day number (e.g., "12")
            last_start_parts = last_start.split(" ")
            if len(last_start_parts) >= 1:
                last_month = last_start_parts[0]  # Get month from start of last week
                last_end = f"{last_month} {last_end}"
        
        overall_date_range = f"{first_start} - {last_end}"
        print(f"Overall date range for email: {overall_date_range}")
    elif len(week_dates) == 1:
        overall_date_range = week_dates[0]
    else:
        print("Could not determine date range for email subject")
        return
    
    # Create email draft
    print(f"\nCreating email draft for Invoice #{invoice_no}...")
    draft_id = create_email_draft(
        invoice_no=invoice_no,
        date_range=overall_date_range,
        pdf_path=pdf_path,
        timesheet_images=timesheet_images
    )
    
    if draft_id:
        print(f"Email draft created successfully with ID: {draft_id}")
        print(f"Subject: Salary Invoice #{invoice_no}, {overall_date_range}")
        print(f"Attachments: {len(timesheet_images) + 1} files (1 PDF + {len(timesheet_images)} timesheets)")
    else:
        print("Failed to create email draft")
if __name__ == '__main__':
    args = sys.argv[1:]
    
    commands = {
        'create_draft_for_latest_invoice': create_draft_for_latest_invoice,
        'main': main
    }
    
    command = args[0] if args else 'main'
    
    if command in commands:
        commands[command]()
    else:
        print(f"Unknown command: {command}")
        print("Available commands:", list(commands.keys()))
