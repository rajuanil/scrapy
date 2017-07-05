from urlparse import urljoin, urlparse

import re
from scrapy.selector import Selector
from scrapy import Request
from scrapy.item import Item, Field
from scrapy.loader import ItemLoader
from scrapy.loader.processors import Identity
from scrapy.spiders.crawl import CrawlSpider


__author__ = 'ttomlins'


class NormalizedJoin(object):
    """ Strips non-empty values and joins them with the given separator. """

    def __init__(self, separator=u' ', return_list=False):
        self.separator = separator
        self.return_list = return_list

    def __call__(self, values):
        result = self.separator.join(
            [value.strip() for value in values if value and not value.isspace()])
        if self.return_list:
            return [result]
        else:
            return result


class JobItem(Item):
    # required fields
    title = Field()
    # a unique id for the job on the crawled site.
    job_id = Field()
    # the url the job was crawled from
    url = Field()
    # name of the company where the job is.
    company = Field()

    # location of the job.
    # should ideally include city, state and country.
    # postal code if available.
    # does not need to include street information
    location = Field()
    description = Field()

    # the url users should be sent to for viewing the job. Sometimes
    # the "url" field requires a cookie to be set and this "apply_url" field will be differnt
    # since it requires no cookie or session state.
    apply_url = Field()

    # optional fields
    industry = Field()
    baseSalary = Field()
    benefits = Field()
    requirements = Field()
    skills = Field()
    work_hours = Field()
    job_type = Field()
    job_sector = Field()
    contact = Field()


class JobItemLoader(ItemLoader):
    default_item_class = JobItem
    default_input_processor = ItemLoader.default_input_processor
    default_output_processor = ItemLoader.default_output_processor
    # all text fields are joined.
    description_in = Identity()
    description_out = NormalizedJoin()
    requirements_in = Identity()
    requirements_out = NormalizedJoin()
    skills_in = Identity()
    skills_out = NormalizedJoin()
    benefits_in = Identity()
    benefits_out = NormalizedJoin()


REF_REGEX = re.compile(r'\/(\d+)$')

APPEND_GB = lambda x: x.strip() + ", GB"


class SimplyLawJobs(CrawlSpider):
    """ Should navigate to the start_url, paginate through
    the search results pages and visit each job listed.
    For every job details page found, should produce a JobItem
    with the relevant fields populated.

    You can use the Rule system for CrawSpider (the base class)
    or you can manually paginate in the "parse" method that is called
    after the first page of search results is loaded from the start_url.

    There are some utilities above like "NormalizedJoin" and JobItemLoader
    to help making generating clean item data easier.
    """
    start_urls = ["http://www.simplylawjobs.com/jobs"]
    name = 'lawjobsspider'
    site_url = "http://www.simplylawjobs.com"
    
    def parse(self, response):
	main_nodes = response.xpath('//ul[@class="search_rez"]//div[@class="info_box"]')
	for node in main_nodes:
	    link = node.xpath('.//div[@class="buttons"]/a[contains(text(),"View job")]/@href').extract()
            link = self.site_url + link[0]
            yield Request(link, self.parse_lawjobs, meta = {'url':response.url})

	next_pagination = ''.join(response.xpath('//div[@id="pagination"]/a[contains(text(),">>")]/@href').extract())
	if next_pagination:
	    next_pagination = self.site_url + next_pagination
	    yield Request(next_pagination, self.parse)

    def parse_lawjobs(self, response):
	sel = Selector(response)
	url = response.meta['url']
	job_id = url.split('/')[-1]
	load = ItemLoader(JobItem(), sel)
	load.add_xpath('title', '//h1[@class="job_title"]/text()')
	load.add_xpath('company', '//div[@class="columns small-12 medium-4 large-4 details"]//a[@target="_blank"]/text()')
	load.add_value('url', [response.meta['url']])
	load.add_value('apply_url', [response.url])
	load.add_xpath('location', '//strong[contains(text(), "Location: ")]//following-sibling::a[1]/text()')
	load.add_xpath('baseSalary', '//strong[contains(text(), "Salary:")]//following-sibling::text()[1]')
	load.add_xpath('job_type', '//strong[contains(text(), "Job type:")]//following-sibling::text()[1]')
	load.add_xpath('job_sector', '//strong[contains(text(), "Job sector:")]//following-sibling::text()[1]')
	load.add_xpath('contact', '//strong[contains(text(), "Contact:")]//following-sibling::text()[1]')
	load.add_xpath('requirements', '//strong[contains(text(), "Experience:")]//following-sibling::text()[1]')
	load.add_xpath('description', '//div[@class="description allow-bulletpoints hide-for-small"]//text()')
	job_id = re.findall(REF_REGEX, response.url.split('?')[0])
	load.add_value('job_id', job_id)
	load.load_item()
	return load.load_item()
