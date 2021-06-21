import csv
import boto3
from csv import reader
import settings

translate = boto3.client(
    'translate',
    aws_access_key_id=settings.AWS_SERVER_PUBLIC_KEY,
    aws_secret_access_key=settings.AWS_SERVER_SECRET_KEY,
    region_name='eu-west-1'
)

# Translate API
print(translate.translate_text(Text="Buna dimineata", SourceLanguageCode="RO", TargetLanguageCode="EN"))


# Declaring the input file path
# input_file = pd.read_csv('result.csv', index_col=0)
input_file = open("result.csv", 'r', newline='\n')
csv_reader = reader(input_file)
next(csv_reader, None)


# Defining the output file, the output's file headers and the writer we'll use to create the file
output_file = open("translated_text.csv", "w", newline='\n')
headers = ["Country Code", "Detected Source Language", "Keyword", "Translated Keyword", "Translated", "Volume"]
writer = csv.DictWriter(output_file, fieldnames=headers)

allowed_languages = {
    "GB": ["en"],
    "US": ["en", "es", "fr"],
    "CA": ["en", "fr"],
    "NL": ["nl", "en"],
    "NZ": ["en"],
    "ZA": ["en"],
    "AT": ["de"],
    "DE": ["de"],
    "IN": ["en"],
    "IE": ["en"],
    "SW": ["en", "fr", "de", "it"],
}


def main():
    """
            Taking a  keyword found in input_file, use the translator object to translate the keyword
            which will be written in a new file
        """
    # Searching for keywords in input file
    for row in csv_reader:
        source_language = row[5]
        keyword = row[2]
        country_code = row[1]
        if country_code in allowed_languages:

            if source_language not in allowed_languages[country_code]:
                source_language = allowed_languages[country_code][0]

            print(row)
            print(f"Source Language: {source_language}")
            print(f"Keyword: {keyword}")
            result = translate.translate_text(Text=keyword, SourceLanguageCode=source_language, TargetLanguageCode="EN")
            print(f'Translated text: {result["TranslatedText"]}')


if __name__ == '__main__':
    main()
