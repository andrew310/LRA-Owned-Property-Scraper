import requests
import re
import csv
from scrapy.http import HtmlResponse
from scrapy.selector import Selector


def dump_csv(properties):
    with open('stl_properties_by_ward.csv', 'w') as csvfile:
        fieldnames = [
                "address",
                "price",
                "zip_code",
                "sqft",
                "land_use",
                "ward",
                "realtor",
                "parcel_id",
        ]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for x in properties:
            writer.writerow(x)


class SessionMixin:

    def get(self, url):
        return self.session.get(url)

    def create_session(self):
        headers = {
            'Accept': 'text/html,application/xhtml+xml, \
                application/xml;q=0.9,*/*;q=0.8',
            'User-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_4) \
                AppleWebKit/537.36 (KHTML, like Gecko) \
                Chrome/28.0.1500.95 Safari/537.36',
            'Accept-encoding': 'gzip,deflate',
            'Connection': 'keep-alive',
            'Accept-Landuage': 'en-US,en;q=0.8',
            'Cache-Control': 'max-age=0',
        }

        session = requests.Session()
        session.headers.update(headers)
        return session


class ScrapeProperties(SessionMixin):

    def __init__(self):
        self.session = super(ScrapeProperties, self).create_session()
        self.get = super(ScrapeProperties, self).get

    @staticmethod
    def get_selector(data):
        if data is None:
            return None

        response = HtmlResponse(url=data.url, body=data.text,
                                headers=data.headers.items(), encoding='utf-8')
        return Selector(response=response)

    def execute(self, action):
        ward = 1

        all_properties = list()

        while ward:
            ward_properties = self.paginate(ward)
            count = len(ward_properties)
            all_properties += ward_properties
            print('%s properties found for ward %s' % (count, ward))

            if count > 0:
                ward += 1
            else:
                ward = 0

        # Call callback function
        action(all_properties)

    def paginate(self, ward):
        row = 1
        page = 1

        page_properties = list()

        url = (
            'https://www.stlouis-mo.gov/government/departments/'
            'sldc/real-estate/lra-owned-property-search.cfm'
            '?startRow={row}&&ward={ward}&Usagew=All'
        )

        while row:
            print('scraping page %s of ward %s' % (page, ward))
            res = self.get(url.format(row=row, ward=ward))
            properties = self.parse_properties(res, ward)
            page_properties += properties
            # print('%s properties scraped' % len(properties))
            # print('row %s' % row)
            # print('page %s' % page)

            # Control
            if len(properties) > 0:
                row += 27
                page += 1
            else:
                row = 0

        return page_properties

    def parse_properties(self, payload, ward):
        container_xpath = (
            "//div[contains(@class, 'large-photo-button-container-flex')]"
            "//a[contains(@class, 'large-photo-button')]"
        )

        properties_so_far = list()
        selector = self.get_selector(payload)
        properties = selector.xpath(container_xpath)

        for x in properties:
            prop = self.parse_single_property(x, ward)
            properties_so_far.append(prop)

        return properties_so_far

    def parse_single_property(self, property, ward):
        address_xpath = (
            ".//*[contains(@class, 'large-photo-button-title')]/text()"
        )

        # Contains 3 items: realtor, zip_code and sqft
        description_xpath = (
            ".//*[contains(@class, 'large-photo-button-description small')]"
            "/text()"
        )

        price_xpath = (
            ".//*[(contains(@class, 'aside'))]/text()"
        )

        land_use_xpath = (
            ".//*[contains(@class, 'large-photo-button-description small')]"
            "//strong/text()"
        )

        parcel_id_xpath = (
            ".//@href"
        )

        address = property.xpath(address_xpath).extract()[0]

        description = property.xpath(description_xpath).extract()[0]
        land_use = property.xpath(land_use_xpath).extract()[0]
        price = property.xpath(price_xpath).extract()[0]
        parcel_id = property.xpath(parcel_id_xpath).extract()[0]

        # Separate by bangs
        description_split = description.split('|')
        description_clean = [x.strip().strip('\n') for x in description_split]

        # Unpack description items
        realtor = description_clean[0]
        zip_code = description_clean[1]
        sqft = description_clean[2]

        # Clean off the dollar sign
        price_clean = re.sub(r"[^0-9.]", "", price)

        parcel_id_clean = parcel_id.split('=')[-1]

        prop = {
            "address": address.strip('\n').strip(),
            "price": price_clean,
            "zip_code": zip_code,
            "sqft": re.sub(r"[^0-9]", "", sqft),
            "land_use": land_use,
            "ward": ward,
            "realtor": realtor,
            "parcel_id": parcel_id_clean,
        }

        return prop


grab = ScrapeProperties()
grab.execute(dump_csv)
