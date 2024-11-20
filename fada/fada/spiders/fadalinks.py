import scrapy

class FadavendaSpider(scrapy.Spider):
    name = "fadalinks"
    allowed_domains = ["fadaimoveis1.com.br"]
    start_urls = [
        "https://fadaimoveis1.com.br/busca/?finalidade=Venda&pagina=1",
        "https://fadaimoveis1.com.br/busca/?finalidade=Aluguel&pagina=1",
        "https://fadaimoveis1.com.br/busca/?finalidade=Temporada&pagina=1"
        ]

    def parse(self, response):
        links = response.xpath('//div[@class="property-thumb-info"]//a[contains(@href, "imovel") and not(contains(@href, "anuncie-seu-imovel"))]/@href').getall()

        for link in links:
            yield {
                'link' : link.split('/imovel/')[0] + '/imovel/' + link.split('/imovel/')[1].split('/')[0]
            }

        current_page = int(response.url.split("pagina=")[-1])  
        next_page = response.url.replace(f"pagina={current_page}", f"pagina={current_page + 1}")
        
        
        if links:  
            yield scrapy.Request(url=next_page, callback=self.parse)
    