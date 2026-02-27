import pandas as pd
import requests
from bs4 import BeautifulSoup
import os
import time

def scrape_job_postings(input_csv, output_dir):
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    output_structured = os.path.join(output_dir, 'job_postings.csv')
    output_text = os.path.join(output_dir, 'job_postings_full_text.csv')

    print(f"Reading URLs from {input_csv}...")
    try:
        df = pd.read_csv(input_csv)
    except Exception as e:
        print(f"Error reading {input_csv}: {e}")
        return

    if 'url' not in df.columns:
        print("Error: The input CSV must contain a 'url' column.")
        return

    structured_data = []
    full_text_data = []

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }

    print(f"Found {len(df)} URLs. Starting scraping process...")
    
    for index, row in df.iterrows():
        url = row['url']
        print(f"Scraping [{index+1}/{len(df)}]: {url}")
        
        try:
            response = requests.get(url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract basic title
            title_tag = soup.find('h1')
            title = title_tag.get_text(strip=True) if title_tag else "N/A"
            
            # Extracting all meaningful text, skip script/styles
            for script in soup(["script", "style", "nav", "footer"]):
                script.extract()
                
            text = soup.get_text(separator=' ', strip=True)
            
            # Basic status check
            status = 'Closed' if 'já foi preenchida' in text.lower() else 'Open'
            
            structured_data.append({
                'url': url,
                'title': title,
                'status': status
            })
            
            full_text_data.append({
                'url': url,
                'title': title,
                'full_text': text
            })
            
            # Pause between requests
            time.sleep(1)
            
        except Exception as e:
            print(f"Failed to scrape {url}: {e}")
            structured_data.append({'url': url, 'title': 'Error', 'status': 'Error'})
            full_text_data.append({'url': url, 'title': 'Error', 'full_text': str(e)})

    # Save data
    pd.DataFrame(structured_data).to_csv(output_structured, index=False)
    pd.DataFrame(full_text_data).to_csv(output_text, index=False)

    print(f"\nScraping complete. Results saved in {output_dir}")
    print(f"- {output_structured}")
    print(f"- {output_text}")

if __name__ == "__main__":
    # Define paths relative to the script's execution location (root of project)
    INPUT_CSV = os.path.join('data', 'raw', 'Job_openings.csv')
    OUTPUT_DIR = os.path.join('data', 'processed')
    scrape_job_postings(INPUT_CSV, OUTPUT_DIR)
