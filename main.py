import json
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


comprehend = boto3.client(
    service_name='comprehend',
    aws_access_key_id=settings.AWS_SERVER_PUBLIC_KEY,
    aws_secret_access_key=settings.AWS_SERVER_SECRET_KEY,
    region_name='eu-west-1')


# Detect language
sentence = "xetra öffnungszeiten"
comprehend_result = json.loads(json.dumps(comprehend.detect_dominant_language(Text=sentence), sort_keys=True, indent=4))
detected_lang = comprehend_result["Languages"][0]["LanguageCode"]
print(detected_lang)

# Translate API
print(translate.translate_text(Text=sentence, SourceLanguageCode=detected_lang, TargetLanguageCode="EN"))


# Declaring the input file path
input_file = open("new_merged_file.csv", 'r', newline='\n')
csv_reader = reader(input_file)
next(csv_reader, None)


allowed_languages = {
    "GB": ["en"],
    "US": ["en", "es", "fr"],
    "CA": ["fr", "en"],
    "NL": ["nl", "en"],
    "NZ": ["en"],
    "ZA": ["en"],
    "AT": ["de"],
    "DE": ["de"],
    "IN": ["en"],
    "IE": ["en"],
    "CH": ["de", "fr", "it"],
}


def main():
    """
            Taking a  keyword found in input_file, use the translator object to translate the keyword
            which will be written in a new file
        """
    # Defining the output file, the output's file headers and the writer we'll use to create the file
    with open("translated_text.csv", "w", newline='\n') as output_file:
        headers = ["Country Code", "Detected Source Language", "Keyword", "Translated Keyword", "Volume"]
        writer = csv.writer(output_file, delimiter=',')
        writer.writerow(headers)

        # Searching for keywords in input file
        for row in csv_reader:
            keyword = row[1]
            country_code = row[14]
            volume = row[5]

            comprehend_result = json.loads(
                json.dumps(comprehend.detect_dominant_language(Text=keyword), sort_keys=True, indent=4))
            detected_language = comprehend_result["Languages"][0]["LanguageCode"]

            if detected_language in allowed_languages[country_code][0]:

                # print(row)
                # print(f"Source Language: {source_language}")
                # print(f"Keyword: {keyword}")
                result = translate.translate_text(Text=keyword,
                                                  SourceLanguageCode=detected_language,
                                                  TargetLanguageCode="EN")
                print(f'Keyword: {keyword}')
                print(f'Detected language: {detected_language}')
                print(f'Translated text: {result["TranslatedText"]}')

                if keyword == result["TranslatedText"]:
                    detected_language = "en"
                    print(row)
                    print(f"Source Language: {detected_language}")
                    print(f"Keyword: {keyword}")

                # Using the writer, we are writing the new 3 columns to the file
                new_row = []
                new_row.insert(0, country_code)
                new_row.insert(1, detected_language)
                new_row.insert(2, keyword)
                new_row.insert(3, result["TranslatedText"])
                new_row.insert(4, volume)
                writer.writerow(new_row)


if __name__ == '__main__':
    main()
