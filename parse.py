import sys
from bs4 import BeautifulSoup
import requests
from xmljson import badgerfish as bf
from xml.etree.ElementTree import fromstring
import json
import time
import csv
import betacode.conv

REQUESTS_PER_SEC = 20
FIELDS = ['id', 'cap', 'verse', 'line', 'index', 'word', 'lang', 'form', 'lemma', 'expandedForm', 'pos', 'person', 'number', 'tense', 'mood', 'voice', 'dialect', 'feature','case','gender',"degree", "len",'mintLabel', 'startDate', 'endDate', 'legend', 'denLabel']
LIMIT = False #20

def bezae_codex_extractor(soup):
    corpus = soup.find(id="Matthew")
    return corpus.text

def read_html_file(filename):
    with open(filename) as f:
        return f.read()


def parse_html(html_doc):
    soup = BeautifulSoup(html_doc, 'html.parser')
    return soup


def convert_html(filename):
    html_doc = read_html_file(filename)
    soup = parse_html(html_doc)
    text = bezae_codex_extractor(soup)
    with open(filename.split(".")[0]+".txt","w") as f:
        f.write(text)

def read_txt(filename):
    for line in open(filename):
        yield line.strip()


def split_legend(legend_name, datum, word_dict):
    legend_list = []
    for i, word in enumerate(datum[legend_name].split()):
        word_dict[word] = {"freq": word_dict.get(word,{}).get("freq",0) + 1}
        legend_list.append({
            "index":i+1,
            "word":word,
            "legend": legend_name,
            "startDate": datum["startDate"],
            "endDate":datum["endDate"],
            "mintLabel":datum["mintLabel"],
            "denLabel":datum["denLabel"],
        })
        # legend_dict[f"word_{legend_name}_{i}"] = word
    return legend_list

def split_words_coins(file_reader):
    word_dict = {}
    lines = []

    for datum in file_reader:
        lines.extend(split_legend("reVerseLegend",datum,word_dict))
        lines.extend(split_legend("obVerseLegend",datum,word_dict))

    return lines, word_dict



def split_words(filename):

    lines = []
    parse = False
    line =0
    index = 1
    verse =1
    cap = 1
    prev_is_blank = False
    word_dict = {}

    # start_word = "1:"
    start_word = ""

    y = 0
    for i in read_txt(filename):
        if start_word == i:
            parse = True
        
        if not parse:
            pass

        # two blank lines
        elif i == "" and prev_is_blank:
            prev_is_blank = False
            line +=1
        # blank line
        elif i == "":
            prev_is_blank = True
        # new verse
        elif i.isdigit() and prev_is_blank:
            verse = i
            line += 1
            index = 1
        # new line 
        elif i.isdigit():
            line = int(i)
            prev_is_blank = True
            index = 1
        # new chapter 
        elif ":" in i:
            cap = i.split(':')[0]
            prev_is_blank = True
        else:
            prev_is_blank = False
            lines.append({
                "id": f"{cap}-{verse}-{line}-{index}",
                "cap": cap,
                "verse": verse,
                "line": line,
                "index":index,
                "word": i,
            })
            word_dict[i] = {"freq": word_dict.get(i,{}).get("freq",0) + 1}
            index +=1

        # Set Limit Here for Testing
        y +=1 
        if LIMIT and y == LIMIT:
            break

    print(f"parsed {len(lines)} words in {filename}")
    return lines, word_dict


def xml_to_json(xml_str):
    try:
        d = bf.data(fromstring(xml_str))
        return json.loads(json.dumps(d))
    except:
        return {}


def greek_to_beta_code(word):
    return betacode.conv.uni_to_beta(word)

def translate_word(word, lang="la"):

    if lang == "greek":
        word = greek_to_beta_code(word)

    params = {
        "lang":lang,
        "lookup":word
    }
    r = requests.get('http://www.perseus.tufts.edu/hopper/xmlmorph',params=params)
    return r.text

def extract_analysis(analyses):
    extracted_analysis = {}

    if not analyses:
        return extracted_analysis
    
    if isinstance(analyses["analysis"], list):
        analysis = analyses["analysis"][0]
        # TODO: add support for multiple lines
        extracted_analysis["len"] = len(analyses["analysis"])
    else:
        analysis = analyses["analysis"] 
        extracted_analysis["len"] = 1

    for k,v in analysis.items():
        extracted_analysis[k] = v.get("$",None)

    return extracted_analysis

def add_translations(words, word_dict, lang="la"):

    for w_obj in words:
        word = w_obj["word"]
        if word_dict[word].get("translations"):
            w_obj.update(**word_dict[word].get("translations"))
        else:
            xml_str = translate_word(word, lang=lang)
            translation_obj = xml_to_json(xml_str)
            analysis = extract_analysis(translation_obj.get("analyses"))
            w_obj.update(**analysis)
            word_dict[word]["translations"] = analysis
            word_dict[word]["raw"] = translation_obj.get("analyses")
            time.sleep(1/REQUESTS_PER_SEC)

    return words, word_dict


def write_csv(json_list, fields, filename):
    with open("dump.json", "w") as g:
        g.write(json.dumps(json_list))
    with open(filename,"w",newline="") as f:  
        cw = csv.DictWriter(f,fieldnames=fields)
        cw.writeheader()
        cw.writerows(json_list)

def write_word_translatations(json_words, filename):
    with open(filename, "w") as f:
       f.write(json.dumps(json_words))


SPLITTERS = {
    "coins" : split_words_coins,
    "default" : split_words
}


if __name__ == "__main__":

    if not len(sys.argv) >1:
        raise ValueError("missing html or txt file")
    filename = sys.argv[1]
    reader = None

    if "html" in filename:
        convert_html(html_file)

    # if "txt" in filename:
    if sys.argv[2]:
        lang = sys.argv[2]
    else:
        "la" 

    if sys.argv[3]:
        splitter = SPLITTERS.get(sys.argv[3],"default")
    else:
        splitter = SPLITTERS["default"]

    if "csv" in filename:
        with open(filename) as f:
            reader = csv.DictReader(f)

            prefix = filename.split(".")[0]
            words, word_dict = splitter(reader or filename)
            t_words, t_word_dict = add_translations(words, word_dict,lang)
            write_csv(t_words,FIELDS, prefix + '-output.csv'),
            write_word_translatations(t_word_dict,prefix + "-words.json" )