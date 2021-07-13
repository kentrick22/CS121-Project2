'''
Group 44:
Keslin Phan 
UCInetID: keslinp

Edriana Tanowidjaja
UCInetID: etanowid

Kentrick Kepawitono
UCInetID: kkepawit
'''

import logging
import re
from urllib.parse import urlparse
from urllib.parse import urljoin

#personal imports
from difflib import SequenceMatcher
from bs4 import BeautifulSoup
import requests

logger = logging.getLogger(__name__)

class Crawler:
    """
    This class is responsible for scraping urls from the next available link in frontier and adding the scraped links to
    the frontier
    """

    def __init__(self, frontier, corpus):
        self.frontier = frontier
        self.corpus = corpus

        # Keep track of the history of the url queries to check for similarities with future urls
        self.checkSimilar = []

        # Analytics #1. keep track of subdomains.
        self.subdomainCount = {}    # {subdomain : num of diff URLs visited in this subdomain}

        # Analytics #2. keep track of a webpage's valid out link count {webpage : num of valid out links}
        self.webpageOutLinkCount = {}
        self.highestWebpage = {}

        # Analytics #3: List of downloaded URLs and identified traps.
        self.downloadedUrls = []
        self.crawlerTraps = {}

        # Analytics #4: Dictionary containing a url and no_of_words of the longest page
        self.longest_page = {
            "url": None,
            "no_of_words": 0
        }

        # Analytics 5: self.frequency_dict containing list of words found except self.stop_words
        self.frequency_dict = {}
        self.stop_words = ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', "aren't", 'as', 'at', 'be', 'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', "can't", 'cannot', 'could', "couldn't", 'did', "didn't", 'do', 'does', "doesn't", 'doing', "don't", 'down', 'during', 'each', 'few', 'for', 'from', 'further', 'had', "hadn't", 'has', "hasn't", 'have', "haven't", 'having', 'he', "he'd", "he'll", "he's", 'her', 'here', "here's", 'hers', 'herself', 'him', 'himself', 'his', 'how', "how's", 'i', "i'd", "i'll", "i'm", "i've", 'if', 'in', 'into', 'is', "isn't", 'it', "it's", 'its', 'itself', "let's", 'me', 'more', 'most', "mustn't", 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on', 'once', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 'same', "shan't", 'she', "she'd", "she'll", "she's", 'should', "shouldn't", 'so', 'some', 'such', 'than', 'that', "that's", 'the', 'their', 'theirs', 'them', 'themselves', 'then', 'there', "there's", 'these', 'they', "they'd", "they'll", "they're", "they've", 'this', 'those', 'through', 'to', 'too', 'under', 'until', 'up', 'very', 'was', "wasn't", 'we', "we'd", "we'll", "we're", "we've", 'were', "weren't", 'what', "what's", 'when', "when's", 'where', "where's", 'which', 'while', 'who', "who's", 'whom', 'why', "why's", 'with', "won't", 'would', "wouldn't", 'you', "you'd", "you'll", "you're", "you've", 'your', 'yours', 'yourself', 'yourselves', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z']


    def start_crawling(self):
        """
        This method starts the crawling process which is scraping urls from the next available link in frontier and adding
        the scraped links to the frontier
        """
        validOutLinkCount = 0
        while self.frontier.has_next_url():
            url = self.frontier.get_next_url()
            logger.info("Fetching URL %s ... Fetched: %s, Queue size: %s", url, self.frontier.fetched, len(self.frontier))
            url_data = self.corpus.fetch_url(url)

            #Analytics 4 & 5
            if(url_data["content_type"] != None and 'html' in str(url_data["content_type"])):
                self.determine_longest_page_and_common_words(url_data["url"], url_data["content"])

            for next_link in self.extract_next_links(url_data):
                if self.is_valid(next_link):
                    if self.corpus.get_file_name(next_link) is not None:
                        self.frontier.add_url(next_link)
                        # Analytics #3
                        self.downloadedUrls.append(next_link)
                        # Analaytics #2
                        validOutLinkCount += 1


            # analytics #2
            self.webpageOutLinkCount[url] = validOutLinkCount
            validOutLinkCount = 0

        self.countSubdomain()                #analytics #1
        self.findHighestOutLinksWebpage()    #analytics #2

    def extract_next_links(self, url_data):
        """
        The url_data coming from the fetch_url method will be given as a parameter to this method. url_data contains the
        fetched url, the url content in binary format, and the size of the content in bytes. This method should return a
        list of urls in their absolute form (some links in the content are relative and needs to be converted to the
        absolute form). Validation of links is done later via is_valid method. It is not required to remove duplicates
        that have already been fetched. The frontier takes care of that.

        Suggested library: lxml
        """
        #outputLinks contain all absolute URLs
        outputLinks = []


        if url_data["http_code"] != 404:
            #Read the content of url and save it to 'content' var
            content = url_data["content"]
            content = BeautifulSoup(content, 'html.parser')

            #Find all URLs and make them absolute URLs
            for link in content.find_all('a'):
                if url_data["is_redirected"] == True:
                    outputLinks.append(urljoin(url_data["final_url"], link.get('href')))
                else:
                    outputLinks.append(urljoin(url_data["url"], link.get('href')))


        return outputLinks

    def is_valid(self, url):
        """
        Function returns True or False based on whether the url has to be fetched or not. This is a great place to
        filter out crawler traps. Duplicated urls will be taken care of by frontier. You don't need to check for duplication
        in this method
        """

        parsed = urlparse(url)
        if parsed.scheme not in set(["http", "https"]):
            self.crawlerTraps[url] = "Invalid scheme"
            return False
        try:
            #Add code here
            #invalid hostname and extension
            if(".ics.uci.edu" in parsed.hostname \
                   and not re.match(".*\.(css|js|bmp|gif|jpe?g|ico" + "|png|tiff?|mid|mp2|mp3|mp4" \
                                    + "|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf" \
                                    + "|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso|epub|dll|cnf|tgz|sha1" \
                                    + "|thmx|mso|arff|rtf|jar|csv" \
                                    + "|rm|smil|wmv|swf|wma|zip|rar|gz|pdf)$", parsed.path.lower())):
                #check for long url
                #Cite source: https://serpstat.com/blog/how-long-should-be-the-page-url-length-for-seo/
                #             https://help.dragonmetrics.com/en/articles/213691-url-too-long#:~:text=A%20URL%20is%20considered%20too,is%20longer%20than%20100%20characters.&text=An%20overly%20long%20URL%20can,of%20the%20total%20URL%20text. 
                if len(url) > 200:
                    self.crawlerTraps[url] = "Long URL"
                    return False

                #check for repeating directories
                #Cite source: https://support.archive-it.org/hc/en-us/articles/208332963-Modify-your-crawl-scope-with-a-Regular-Expression#RepeatingDirectories
                if re.match(r"^.*?(/.+?/).*?\1.*$|^.*?/(.+?/)\2.*$", parsed.path.lower()):
                    self.crawlerTraps[url] = "Repeating directories"
                    return False

                #check for page reference
                if ('#' in url):
                    self.crawlerTraps[url] = "Keeps crawler trap on the same webpage"
                    return False

                #check for dynamic url/history based traps
                if (('?' in url) or ('&' in url)) and ('=' in url):
                    self.checkSimilar.append(urlparse(url).query)
                    if len(self.checkSimilar) > 1:
                        for q in self.checkSimilar[:-1]:
                            if SequenceMatcher(None, q, urlparse(url).query).ratio() > 0.80:
                                self.crawlerTraps[url] = "Dynamic URL/History detection trap"
                                return False

                # check for similar URLs (avoid getting stuck in classes/calendar)
                if len(self.downloadedUrls) > 2:
                    if SequenceMatcher(None, self.downloadedUrls[-2], url).ratio() > 0.92:
                        self.crawlerTraps[url] = "Check for similar URLs"
                        return False

                return True
            self.crawlerTraps[url] = "Invalid Hostname and extension"
            return False

        except TypeError:
            print("TypeError for ", parsed)
            return False

    def determine_longest_page_and_common_words(self, url, content):
        """
        Analytics #4 & #5.
        This function takes care Analytics #4 and #5. It takes arguments url and content.
        It first  tokenize the content into words and then determine the longest page in terms
        of number of words (Analytics #4) and determine 50 most common words in the entire set
        of pages excluding the stop_words (Analytics #5)
        """

        final_string = []
        soup = BeautifulSoup(content, 'html.parser')
        text = soup.get_text().lower()

        for i in text:
            if i in 'abcdefghijklmnopqrstuvwxyz1234567890 ':
                final_string.append(i)
            elif i not in 'abcdefghijklmnopqrstuvwxyz1234567890 ':
                final_string.append(' ')

        #Join all characters together and then split by whitespace
        final_string = "".join(final_string)
        final_string = list(final_string.split(" "))
        #Remove unnecessary whitespaces left
        final_string = [sub for sub in final_string if sub != '']

        #Determine which one is the longest page
        if len(final_string) > self.longest_page["no_of_words"]:
            self.longest_page["url"] = url
            self.longest_page["no_of_words"] = len(final_string)

        #Add words to self.frequency_dict
        for i in final_string:
            if i not in self.frequency_dict and i not in self.stop_words and i.isdigit() == False:
                self.frequency_dict[i] = 1
            elif i in self.frequency_dict and i not in self.stop_words and i.isdigit() == False:
                self.frequency_dict[i] += 1

    def findHighestOutLinksWebpage(self):
        """
        Analytics #2.
        Find a list of webpage(s) with the highest number of valid out links.
        Returning a list in case of more than one webpage having the same number of valid out links.
        """
        # find the max by values (num of valid out links)
        if len(self.webpageOutLinkCount) > 0:
            maxCount = max(self.webpageOutLinkCount.values())

        # iterate through the dict and see which value has the max value
        # (in case there's another webpage with same number of out link, we want both)
        for webpage,count in self.webpageOutLinkCount.items():
            if count == maxCount:
                self.highestWebpage[webpage] = count

    def countSubdomain(self):
        '''
        Analytics #1.
        Keep track of the subdomains that it visited (kept in self.subdomainCount initialized earlier),
        and count how many different URLs it has processed from each of those subdomains.
        '''
        try:
            for i in set(self.downloadedUrls):
                subdomain = urlparse(i)
                subdomain = subdomain.netloc
                if subdomain not in self.subdomainCount:
                    self.subdomainCount[subdomain] = 1
                elif subdomain in self.subdomainCount:
                    self.subdomainCount[subdomain] += 1
        except:
            pass

    def analyticsToFile(self):
        """
        This function prints all the analytics to corresponding txt files.
        """
        #Analytic #1
        with open('analytics1.txt', 'w', encoding = "utf-8") as writer:
            for k,v in self.subdomainCount.items():
                writer.write("{} {}".format(k, v))
                writer.write('\n')

        #Analytic #2
        with open('analytics2.txt', 'w', encoding = "utf-8") as writer:
            for k,v in self.highestWebpage.items():
                writer.write("{} {}".format(k, v))
                writer.write('\n')

        #Analytic #3
        with open('analytics3a.txt', 'w', encoding = "utf-8") as writer:
            for url in set(self.downloadedUrls):
                writer.write(url)
                writer.write('\n')

        with open('analytics3b.txt', 'w', encoding = "utf-8") as writer:
            for k, v in self.crawlerTraps.items():
                writer.write("URL: {}\nReason: {}\n\n".format(k, v))

        #Analytic #4
        with open('analytics4.txt', 'w', encoding = "utf-8") as writer:
            writer.write(self.longest_page["url"])
            writer.write(' ')
            writer.write(str(self.longest_page["no_of_words"]))

        #Analytic #5
        #Source Cite: https://stackoverflow.com/questions/15371691/how-to-sort-a-dictionary-by-value-desc-then-by-key-asc
        sorted_list = sorted(self.frequency_dict.items(), key=lambda item: (-item[1], item[0]))
        sorted_list = sorted_list[:50]
        with open('analytics5.txt', 'w', encoding = "utf-8") as writer:
            for i in sorted_list:
                writer.write("{} {}".format(i[0], i[1]))
                writer.write('\n')
