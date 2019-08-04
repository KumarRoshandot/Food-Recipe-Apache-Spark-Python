from urllib.request import urlopen
from bs4 import BeautifulSoup
import ssl
import os
import sys

if os.path.exists('jobs.zip'):
    sys.path.insert(0, 'jobs.zip')
else:
    sys.path.insert(0, './jobs')

def get_recipe(url):
    # Ignore SSL certificate errors
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    #url = 'https://thepioneerwoman.com/cooking/spicy-stewed-beef-with-creamy-cheddar-grits/'
    html = urlopen(url, context=ctx).read()
    soup = BeautifulSoup(html, "html.parser")
    # Retrieve all of the anchor tags
    mydivs = soup.find("div", {"class": "entry-content"})
    tags = mydivs.find_all('p')
    recipe_content = []
    for tag in tags:
        # Look at the parts of a tag
        recipe_content.append(tag.get_text().strip())

    return str(recipe_content).lstrip('[').rstrip(']')

