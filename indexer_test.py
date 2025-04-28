import boto3
import json
import os

import time
import logging
import re
import nltk

from whoosh.index import create_in, open_dir, exists_in
from whoosh.fields import Schema, ID, TEXT, DATETIME
from whoosh.qparser import QueryParser

nltk.download('stopwords')
nltk.download('wordnet')
nltk.download('omw-1.4')

from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

STOPWORDS = set(stopwords.words('english'))
lemmatizer = WordNetLemmatizer()


index_dir = "index_dir"

schema = Schema(
    url=ID(stored=True, unique=True),
    content=TEXT(stored=True)
)



def initialize_index():
    if not os.path.exists(index_dir):
        os.makedirs(index_dir)  # Create the folder if it doesn't exist
    if exists_in(index_dir):
        return open_dir(index_dir)
    return create_in(index_dir, schema)



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - Indexer - %(levelname)s - %(message)s')

def preprocessing(text):
    text = text.lower()
    text = clean_text(text)
    tokens = text.split()
    tokens = [word for word in tokens if word not in STOPWORDS]
    lemmatized_tokens = [lemmatizer.lemmatize(token) for token in tokens]
    return " ".join(lemmatized_tokens)

def clean_text(text):
    text = re.sub(r'<[^>]+>', '', text) # Remove HTML tags
    text = re.sub(r'[^a-z\s0-9]', '', text) # Remove special characters and punctuation
    text = re.sub(r'\s+', ' ', text).strip() # Remove extra spaces
    return text



def search_index(ix, query_str):
    processed_query = preprocessing(query_str)

    with ix.searcher() as searcher:
        # Define the query parser and specify the fields to search (e.g., title, content)
        query_parser = QueryParser("content", schema=ix.schema)
        query = query_parser.parse(processed_query)
        
        # Perform the search
        results = searcher.search(query, limit=None)  # Adjust limit based on your needs

        # Return results as a list of dictionaries
        search_results = [
            {"url": result["url"], "content": result["content"]}
            for result in results
        ]
        
        return search_results
def indexer_process(input, index, search):
    ix = initialize_index()

    logging.info("Indexer node started and waiting for messages...")
    

    # --- 1. Check for new content to index ---
    if index:

        body = input
        content_to_index = body.get('text_content')
        url_recv = body.get('document_url')

        
        if content_to_index:
            try:
            
                with ix.writer() as writer:
                    writer.add_document(
                        url= url_recv,
                        content=preprocessing(content_to_index)
                    )


                logging.info(f"Successfully indexed content for URL: {url_recv}")              
            except Exception as e:
                logging.error(f"Error indexing content for URL {url_recv}: {e}")               
    

 # --- 2. Check for search requests ---
    if search:
        try:
            results = search_index(ix, input)
            logging.info(f"Processed search query: {input}")
            print([(result['url'], result['content']) for result in results])
        except Exception as e:
            logging.error(f"Error processing search query: {e}")

                
        

if __name__ == '__main__':
    json1 = {
        "document_url": "http://siveen.com",
        "text_content": "Siveen | UX/UI Design • Smart Home • Graphics • Templates The page is currently under construction The page is currently under construction IMPRESSUM (LEGAL NOTICE) • DATENSCHUTZ (PRIVACY POLICY) • COOKIES"
    }
    json2 = {
        "document_url": "https://www.vonwittken.com/kontakt/",
        "text_content": "Kontakt - von Wittken | Layout und Design EST. 2014 | Â© vonwittken.com Wir Ã¼berarbeiten unsere Webseite Relaunch Q1-2025 Hallo! Danke fÃ¼r das Interesse! Frei nach: „Die Schuster tragen die schlechtesten Schuhe“ hatte sich unsere bisherige Webseite seit 2014 wenig weiterentwickelt. Deshalb Ã¼berarbeiten wir jetzt die Inhalte, fegen ordentlich durch und polieren die vielen groÃartigen Referenzen und TrophÃ¤en fÃ¼r unser neues Online-Schaufenster. Der Relaunch steht fÃ¼r 2025 im Kalender, da wir zuallererst IHRE Projekte betreuen. Danke auch fÃ¼r 10 Jahre Vertrauen und Weitersagen â¤ Wenn Sie uns mit neuen Aufgaben von der Renovierung der eigenen Webseite abhalten mÃ¶chten: Frank von Wittken Designer Heidter StraÃe 100B D-42369 Wuppertal +49 (0) 151 65 11 97 29 info @ vonwittken.com Startseite Impressum Datenschutz"
    }
    
    indexer_process(json1, True, False)
    indexer_process(json2, True, False)
    indexer_process("Smart Home", False, True)
    indexer_process("UX/UI Design", False, True)
    indexer_process("unsere", False, True)