from flask import Flask, request, jsonify
from scrapy.crawler import CrawlerRunner
from scrapy.utils.project import get_project_settings
from twisted.internet import reactor
from azure.storage.blob import BlobServiceClient
import re
import string
import os
import scrapy
from flask_cors import CORS

app = Flask(__name__)
CORS(app)


class MainSpider(scrapy.Spider):
    name = 'main_spider'
    start_urls = []
    combined_content = []

    def parse(self, response):
        # Extract the main content of the web page
        main_content = response.xpath('//text()').getall()
        self.combined_content.extend(main_content)

        # Extract hyperlinks from the web page (Level 1)
        for link in response.xpath('//a'):
            link_text = link.xpath('.//text()').get()
            link_url = link.xpath('.//@href').get()

            if link_text and link_url:
                hyperlink_info = f'Hyperlink: {link_text.strip()}: {link_url.strip()}'
                self.combined_content.append(hyperlink_info)

                # Follow the hyperlink and parse its content (Level 2)
                yield response.follow(link_url, callback=self.parse_hyperlink_content)

    def parse_hyperlink_content(self, response):
        # Extract and return the text content of the hyperlink
        link_text = response.xpath('//title/text()').get()  # Extract title if available
        link_content = response.xpath('//text()').getall()

        self.combined_content.append(f'----- Hyperlink Content: {link_text} -----\n')
        self.combined_content.extend(link_content)


def get_valid_filename(name):
    valid_chars = '-_.() %s%s' % (string.ascii_letters, string.digits)
    return ''.join(c if c in valid_chars else '_' for c in name)


def generate_container_name(user_id):
    return f'user-container-{user_id}'


def create_container(connection_string, container_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    try:
        container_client.create_container()
        print(f"Container '{container_name}' created successfully.")
    except Exception as e:
        print(f"Error creating container '{container_name}': {e}")


@app.route('/urldata', methods=['POST'])
def scrape():
    data = request.get_json()
    url = data.get('url')
    user_id = data.get('user_id')

    if not url:
        return jsonify({"error": "URL not provided"}), 400
    if not user_id:
        return jsonify({"error": "User ID not provided"}), 400

    try:
        container_name = generate_container_name(user_id)

        blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=autodemo;AccountKey=vv3+LSjLsvivd2lzKpv2CMlCyBKDmB0dPvB7wtWAKpRWD3NUMwXeCfj7saZojzGg/TkMp6WR+Fql+AStYHqTIg==;EndpointSuffix=core.windows.net")
        blob_name = get_valid_filename(url) + ".txt"

        # Check if the container exists, if not create it
        container_client = blob_service_client.get_container_client(container_name)
        if not container_client.exists():
            create_container("DefaultEndpointsProtocol=https;AccountName=autodemo;AccountKey=vv3+LSjLsvivd2lzKpv2CMlCyBKDmB0dPvB7wtWAKpRWD3NUMwXeCfj7saZojzGg/TkMp6WR+Fql+AStYHqTIg==;EndpointSuffix=core.windows.net", container_name)

        runner = CrawlerRunner(get_project_settings())
        deferred = runner.crawl(MainSpider, start_urls=[url])
        deferred.addBoth(lambda _: reactor.stop())
        reactor.run()

        # Write the combined content to a local file, handling encoding
        output_filename = blob_name
        with open(output_filename, 'w', encoding='utf-8') as output_file:
            output_file.write("\n".join(MainSpider.combined_content))

        # Upload the local file to Blob storage
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        with open(output_filename, 'rb') as file:
            blob_client.upload_blob(file)

        # Clean up the local file after uploading to blob storage
        os.remove(output_filename)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"message": "Scraping process completed and file uploaded"}), 200


if __name__ == '__main__':
    app.run(debug=True, port=8000)
