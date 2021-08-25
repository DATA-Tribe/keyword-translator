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


# Detect language test
sentence = "xetra öffnungszeiten"
comprehend_result = json.loads(json.dumps(comprehend.detect_dominant_language(Text=sentence), sort_keys=True, indent=4))
detected_lang = comprehend_result["Languages"][0]["LanguageCode"]
print(detected_lang)

# Translate API test
print(translate.translate_text(Text=sentence, SourceLanguageCode=detected_lang, TargetLanguageCode="EN"))


allowed_languages = {
    "GB": ["en"],
    "US": ["en", "es", "fr"],
    "CA": ["fr", "en"],
    "NL": ["nl", "en"],
    "NZ": ["en"],
    "ZA": ["en"],
    "AT": ["de", "en"],
    "DE": ["de", "en"],
    "IN": ["en"],
    "IE": ["en"],
    "CH": ["de", "fr", "it", "en"],
    "CO": ["es", "en"],
    "FI": ["fi", "en"],
    "JP": ["ja", "en"],
    "ES": ["es", "en"],
    "AR": ["es", "en"],
    "CL": ["es", "en"],
    "EC": ["es", "en"],
    "PE": ["es", "en"],
    "MX": ["es", "en"],
    "BR": ["pt", "en"],
    "PT": ["pt", "en"],
    "IT": ["it", "en"],
    "DK": ["da", "en"],
    "NO": ["no", "en"],
    "SE": ["sv", "en"],
}


def main():
    """
            Taking a  keyword found in input_file, use the translator object to translate the keyword
            which will be written in a new file
        """

    # Declaring the input file path
    # encoding='cp1252'
    with open("missingkws3_utf8.csv", 'r', newline='\n') as input_file:
        csv_reader = reader(input_file)
        next(csv_reader, None)
        count = 1

        # Defining the output file, the output's file headers and the writer we'll use to create the file
        with open("missingkw_translated_text03.csv", "w", newline='\n') as output_file:
            headers = ["Country Code", "Detected Source Language", "Keyword", "Translated Keyword", "Volume"]
            writer = csv.writer(output_file, delimiter=',')
            writer.writerow(headers)

            # Searching for keywords in input file
            for row in csv_reader:
                keyword = row[1]
                country_code = row[14]
                volume = row[5]
                print(f"Keyword: {keyword}")
                print(f"Country Code: {country_code}")
                print(f"Row Count: {count}")

                # Api call to detect language
                comprehend_result = json.loads(
                    json.dumps(comprehend.detect_dominant_language(Text=keyword), sort_keys=True, indent=4))
                detected_language = comprehend_result["Languages"][0]["LanguageCode"]
                print(f"Detected Language: {detected_language}")

                if detected_language not in allowed_languages[country_code]:
                    detected_language = "en"
                    print("Forcing detected language to EN")

                result = translate.translate_text(
                    Text=keyword,
                    SourceLanguageCode=detected_language,
                    TargetLanguageCode="EN"
                )

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

                count += 1



if __name__ == '__main__':
    main()
