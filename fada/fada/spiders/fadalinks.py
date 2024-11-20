import scrapy


class FadavendaSpider(scrapy.Spider):
    name = "fadalinks"
    allowed_domains = ["fadaimoveis1.com.br"]
    start_urls = ["https://fadaimoveis1.com.br/busca/?finalidade=Aluguel&pagina=1"]

    def parse(self, response):
        links = response.xpath('//div[@class="property-thumb-info"]//a[contains(@href, "imovel") and not(contains(@href, "anuncie-seu-imovel"))]/@href').getall()

        for link in links:
            yield {
                'link' : link
            }
