from bs4 import BeautifulSoup
from bs4.element import Comment
import urllib.request


def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

def text_from_html(body):
    soup = BeautifulSoup(body, 'html.parser')
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)  
    return u" ".join(t.strip() for t in visible_texts)


def get_docs(url_list, dpath="/home/eolus/Desktop/Dauphine/bigdata/tfidf/data"):
	for i, url in enumerate(url_list):
		html = urllib.request.urlopen(url).read()
		text = text_from_html(html)
		fpath = "{}/{}.txt".format(dpath, i)
		with open(fpath, "w") as text_file:
			print(text, file=text_file)


if __name__ == '__main__':

	url_list = ['https://www.nytimes.com/2018/04/06/opinion/tiger-woods-masters-tournament.html']
	get_docs(url_list)
