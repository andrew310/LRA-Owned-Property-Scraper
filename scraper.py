# -*- coding: utf-8 -*-
import requests
import re
import csv
import pickle
from collections import defaultdict
from scrapy.http import HtmlResponse
from scrapy.selector import Selector
from concurrent.futures import ThreadPoolExecutor

thread_pool = ThreadPoolExecutor(25)


def threaded(fn):
    def wrapper(*args, **kwargs):
        return thread_pool.submit(fn, *args, **kwargs)
    return wrapper


def save_pickle(ward, properties):
    pickle.dump(properties, open( "save_%s.p" % ward, "wb" ))


def dump_csv(properties, max_permits):
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
                "zoning",
                "owner",
        ]

        for i in range(max_permits):
            fieldnames.append('Permit %d Owner Name' % (i + 1))
            fieldnames.append('Permit %d Permit Type' % (i + 1))
            fieldnames.append('Permit %d Application Date' % (i + 1))
            fieldnames.append('Permit %d Completion Date' % (i + 1))
            fieldnames.append('Permit %d Issued Date' % (i + 1))
            fieldnames.append('Permit %d New Use' % (i + 1))
            fieldnames.append('Permit %d Estimated Costs' % (i + 1))
            fieldnames.append('Permit %d Description' % (i + 1))

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for x in properties:
            writer.writerow(x)


def transpose(rows):
    max_permits = max([len(x['permits']) for x in rows])
    rows_with_permits = list()
    for x in rows:
        x = defaultdict(str, x)
        for idx, val in enumerate(x['permits']):
            t = defaultdict(str, val)
            x['Permit %d Owner Name' % (idx + 1)] = t['Owner Name']
            x['Permit %d Permit Type' % (idx + 1)] = t['Permit Type']
            x['Permit %d Application Date' % (idx + 1)] = t['Application Date']
            x['Permit %d Completion Date' % (idx + 1)] = t['Completion Date']
            x['Permit %d Issued Date' % (idx + 1)] = t['Issued Date']
            x['Permit %d New Use' % (idx + 1)] = t['New Use']
            x['Permit %d Estimated Costs' % (idx + 1)] = t['Estimated Costs']
            x['Permit %d Description' % (idx + 1)] = t['Description']
        x.pop('permits')
        rows_with_permits.append(x)

    return rows_with_permits, max_permits

def get_rows_from_pickles():
    rows = list()
    for i in range(1, 28):
        temp_rows = pickle.load(open('save_%s.p' % i, 'rb'))
        print(len(temp_rows), 'save_%s.p' % i)
        rows += temp_rows
    print(len(rows))
    return rows

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
        # wards = range(1, 28)
        wards = [3]
        for x in wards:
            self.scrape_ward(x, action)

        # Wait for threads to finish before proceeding.
        thread_pool.shutdown(wait=True)

    @threaded
    def scrape_ward(self, ward, action):
        ward_properties = self.paginate(ward)
        count = len(ward_properties)
        print('%s properties found for ward %s' % (count, ward))
        # Call callback function
        action(ward, ward_properties)

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
            properties_with_detail = self.get_details(properties)

            page_properties += properties

            # Control
            if len(properties) > 0:
                row += 27
                page += 1
            else:
                row = 0

        return page_properties

    def get_details(self, properties):
        for prop in properties:
            url = (
                "https://www.stlouis-mo.gov/data/address-search/index.cfm?"
                "parcelid={parcel_id}&firstview=true&categoryBy=form.start,"
                "form.RealEstatePropertyInfor,form.BoundaryGeography,"
                "form.ResidentialServices,form.TrashMaintenance,"
                "form.ElectedOfficialsContacts,form.RealEstatePropertyInfor"
                ",form.BoundaryGeography,form.TrashMaintenance,"
                "form.ElectedOfficialsContacts"
            )

            payload = self.get(url.format(parcel_id=prop['parcel_id']))
            selector = self.get_selector(payload)

            land_use_table = selector.xpath(
                "//*[contains(text(),'Land Use Information')]//..//table"
            )

            property_information_table = selector.xpath(
                "//*[contains(text(),'Property Information')]//..//table"
            )

            permit_table = selector.xpath(
                ".//*[contains(text(),'Permit Type')]//../../../tr"
            )

            try:
                zoning = land_use_table.xpath(
                    "//*[contains(text(), 'Zoning:')]//..//td/text()"
                ).extract()[0]
            except:
                zoning = None

            try:
                land_use = land_use_table.xpath(
                    "//*[contains(text(), 'Land use:')]//..//td/text()"
                ).extract()[0]
            except:
                land_use = None

            try:
                owner = property_information_table.xpath(
                    "//*[contains(text(), 'Owner name:')]//..//td/text()"
                ).extract()[0]
            except:
                owner = None

            permits = self.parse_permits(permit_table)
            prop['permits'] = permits
            prop['zoning'] = zoning
            prop['land_use'] = zoning
            prop['owner'] = zoning

        return properties

    def parse_permits(self, table):
        permits = []

        # If we don't have any
        try:
            headers = table.pop(0).xpath('./th//text()').extract()
        except:
            return list()

        headers = [x.strip() for x in headers if x is not '\n']

        for x in table:
            raw_row = x.xpath('.//td')
            extracted_row = [x.xpath('./text()').extract() for x in raw_row]
            row = list()
            for y in extracted_row:
                if len(y) > 0:
                    row.append(y[0].strip())
                else:
                    row.append('')

            p = {headers[idx]: x.strip() for idx, x in enumerate(row)}

            permits.append(p)

        return permits

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


# grab = ScrapeProperties()
# grab.execute(save_pickle)

rows = get_rows_from_pickles()
transposed, max_permits = transpose(rows)
dump_csv(transposed, max_permits)
