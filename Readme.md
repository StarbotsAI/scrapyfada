### **Srapiando dados da Fada Imovéis**

Para iniciar o scrap dos links ativos no site:
```bash
scrapy crawl fadalinks -o ../data/data.jsonl
```

Para iniciar o tratamento dos dados consultando no banco:
```bash
python scrapyfada/transform/main.py
```