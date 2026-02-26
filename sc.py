import asyncio
import json
import os
import re
from datetime import datetime
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CacheMode
from crawl4ai.deep_crawling import BFSDeepCrawlStrategy
from crawl4ai.content_scraping_strategy import LXMLWebScrapingStrategy
from bs4 import BeautifulSoup


class NurContentExtractor:
    """针对 nur.cn 网站的内容提取器"""
    
    def __init__(self, output_dir):
        self.dataset = []
        self.output_dir = output_dir
        self.visited_urls = set()
        os.makedirs(output_dir, exist_ok=True)
        
    def is_duplicate(self, url):
        """检查URL是否已处理过"""
        if url in self.visited_urls:
            return True
        self.visited_urls.add(url)
        return False
        
    def extract_content(self, html_content, url):
        """从HTML中提取 class="tt" (标题) 和 class="view_p mazmun" (正文) 的纯文本"""
        if not html_content:
            return None
            
        soup = BeautifulSoup(html_content, 'lxml')
        
        title_elem = soup.find('h2', class_='tt')
        title = title_elem.get_text(strip=True) if title_elem else ""
        
        content_elem = soup.find('div', class_='view_p mazmun')
        content = ""
        if content_elem:
            content = content_elem.get_text(separator='\n', strip=True)
        
        if not title and not content:
            return None
            
        return {
            "url": url,
            "title": title,
            "content": content,
            "crawl_time": datetime.now().isoformat(),
            "content_length": len(content)
        }
    
    def save_single_txt(self, data):
        """保存单个文章为 txt 文件"""
        if not data['title']:
            return None
        
        safe_title = re.sub(r'[<>:"/\\|?*]', '_', data['title'])[:50]
        txt_filename = f"{safe_title}.txt"
        txt_path = os.path.join(self.output_dir, txt_filename)
        
        with open(txt_path, 'w', encoding='utf-8') as f:
            f.write(f"{data['title']}\n\n")
            f.write(data['content'])
        
        return txt_filename
    
    def load_existing_urls(self):
        """加载已存在的URL去重"""
        for f in os.listdir(self.output_dir):
            if f.endswith('.json'):
                try:
                    with open(os.path.join(self.output_dir, f), 'r', encoding='utf-8') as fp:
                        data = json.load(fp)
                        if 'articles' in data:
                            for article in data['articles']:
                                self.visited_urls.add(article['url'])
                except:
                    pass


async def deep_crawl_nur(max_pages=50000, max_depth=3):
    """深度爬取 nur.cn 的新闻页面
    
    Args:
        max_pages: 最大爬取页面数 (默认 50000)
        max_depth: 最大爬取深度 (默认 3)
    """
    
    output_dir = "nur_articles"
    extractor = NurContentExtractor(output_dir)
    extractor.load_existing_urls()
    
    print(f"📋 已存在 {len(extractor.visited_urls)} 条记录，将跳过重复内容")
    
    deep_strategy = BFSDeepCrawlStrategy(
        max_depth=max_depth,
        include_external=False,
        max_pages=max_pages,
    )
    
    config = CrawlerRunConfig(
        deep_crawl_strategy=deep_strategy,
        scraping_strategy=LXMLWebScrapingStrategy(),
        cache_mode=CacheMode.BYPASS,
        stream=True,
        verbose=True,
    )
    
    print(f"🚀 开始深度爬取 https://www.nur.cn/")
    print(f"📊 配置: 最大页面={max_pages}, 最大深度={max_depth}")
    print(f"📁 文章将保存到: {output_dir}/")
    print("=" * 60)
    
    async with AsyncWebCrawler() as crawler:
        result_count = 0
        success_count = 0
        skip_count = 0
        
        async for result in await crawler.arun("https://www.nur.cn/", config=config):
            result_count += 1
            
            if result.success and result.html:
                if '/news/' in result.url and result.url.endswith('.shtml'):
                    if extractor.is_duplicate(result.url):
                        skip_count += 1
                        continue
                        
                    data = extractor.extract_content(result.html, result.url)
                    
                    if data:
                        extractor.dataset.append(data)
                        success_count += 1
                        
                        txt_file = extractor.save_single_txt(data)
                        
                        if success_count % 100 == 0:
                            print(f"✅ 已保存 {success_count} 篇: {data['title'][:30]}...")
            
            if result_count % 500 == 0:
                print(f"   📊 进度: {result_count} 页面, {success_count} 成功, {skip_count} 跳过")
    
    print("\n" + "=" * 60)
    print(f"📊 爬取完成!")
    print(f"   - 总页面数: {result_count}")
    print(f"   - 成功提取: {success_count}")
    print(f"   - 跳过重复: {skip_count}")
    
    output_file = save_final_dataset(extractor.dataset, output_dir)
    print(f"\n💾 数据集已保存至: {output_file}")
    
    return extractor.dataset


def save_final_dataset(dataset, output_dir):
    """保存最终数据集到文件"""
    if not dataset:
        print("⚠️ 无新数据需要保存")
        return None
        
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    json_file = os.path.join(output_dir, f"nur_dataset_{timestamp}.json")
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump({
            "source": "https://www.nur.cn/",
            "crawl_date": datetime.now().isoformat(),
            "total_articles": len(dataset),
            "articles": dataset
        }, f, ensure_ascii=False, indent=2)
    
    return json_file


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='爬取 nur.cn 新闻内容')
    parser.add_argument('--pages', type=int, default=50000, help='最大爬取页面数 (默认 50000)')
    parser.add_argument('--depth', type=int, default=3, help='最大爬取深度 (默认 3)')
    
    args = parser.parse_args()
    
    dataset = asyncio.run(deep_crawl_nur(max_pages=args.pages, max_depth=args.depth))
