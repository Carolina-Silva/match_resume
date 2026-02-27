import pandas as pd
import os
import re


def extract_insights(input_file, output_file):
    print(f"Loading data from {input_file}...")
    try:
        df = pd.read_csv(input_file)
    except Exception as e:
        print(f"Error reading {input_file}: {e}")
        return

    if 'full_text' not in df.columns:
        print("Error: The input CSV must contain a 'full_text' column.")
        return

    texts = df['full_text'].dropna().tolist()

    # Define some common tech keywords to look for
    keywords = [
        'python', 'sql', 'aws', 'azure', 'gcp', 'java', 'c#', 'c++', 'javascript', 'typescript',
        'react', 'angular', 'vue', 'node', 'docker', 'kubernetes', 'spark', 'hadoop', 'kafka',
        'git', 'agile', 'scrum', 'inglês', 'ingles', 'excel', 'power bi', 'tableau', 'machine learning',
        'deep learning', 'nlp', 'etl', 'airflow', 'linux', 'bash', 'nosql', 'mongodb', 'postgresql', 'mysql'
    ]

    print("Analyzing job descriptions for keywords...")
    keyword_counts = {}
    
    for text in texts:
        text_lower = text.lower()
        for kw in keywords:
            # Simple boundary check to avoid partial matches
            if re.search(rf'\b{re.escape(kw)}\b', text_lower):
                # normalize ingles to inglês for counting
                key = 'inglês' if kw == 'ingles' else kw
                keyword_counts[key] = keyword_counts.get(key, 0) + 1

    # Convert to DataFrame
    insights_df = pd.DataFrame(keyword_counts.items(), columns=['Keyword', 'Job_Count'])
    insights_df = insights_df.sort_values(by='Job_Count', ascending=False)

    print("\n--- TOP SKILLS & KEYWORDS ---")
    print(insights_df.head(15).to_string(index=False))

    # Save
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    insights_df.to_csv(output_file, index=False)
    print(f"\nInsights saved to {output_file}")


if __name__ == "__main__":
    INPUT_FILE = os.path.join('data', 'processed', 'job_postings_full_text.csv')
    OUTPUT_FILE = os.path.join('data', 'processed', 'insights.csv')
    extract_insights(INPUT_FILE, OUTPUT_FILE)
