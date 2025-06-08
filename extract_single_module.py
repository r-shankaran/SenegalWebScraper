from bs4 import BeautifulSoup
import requests
import pandas as pd
from io import StringIO
from tqdm import tqdm
import nltk

url = 'https://microdata.worldbank.org/index.php/catalog/4297/data-dictionary'
response = requests.get(url)
soup = BeautifulSoup(response.content, features="html.parser")
response = requests.get(url)
df = pd.read_html(response.content)[0]

# get raw text (needed for module descriptions)
for script in soup(["script", "style"]):
    script.extract()
text = soup.get_text() # get text
lines = (line.strip() for line in text.splitlines()) # break into lines and remove leading and trailing space on each
chunks = (phrase.strip() for line in lines for phrase in line.split("  ")) # break multi-headlines into a line each
text = '\n'.join(chunk for chunk in chunks if chunk) # drop blank lines
text = text.splitlines()
print(text)
# Extract module names from df
print("Extracting module names...")
df["module_name"] = df["Data file"].apply(lambda x: x.split()[0])

print("Extracting module descriptions...")
# Extract module descriptions from raw text - if preceding line is an element of module_names, add string to
# module_descriptions. (must do this since there are line breaks that df doesn't recognize)
focus_terms = text[text.index('s00_me_sen2018', 42):]
module_descriptions = []
previous_line = 0
print("focus terms:", focus_terms)
for i in range(1, len(focus_terms)):
    if focus_terms[previous_line] in df["module_name"].values:
        if focus_terms[i].isdigit(): # note: there is no description for module AGSEC8B
            module_descriptions.append("None")
        else:
            module_descriptions.append(focus_terms[i])
    previous_line += 1
print(module_descriptions)
df["module_description"] = module_descriptions
print(df["module_description"])

# get rid of the Data File column in df because it is separated into module name and description now.
df.drop(columns=["Data file"], inplace=True)
main_df = df.copy()
print(main_df["module_name"])
# For every module in LSMS, extract variable_name, variable_description, data_type, and unique_id
all_dictionaries = []
print("Extracting variable names, descriptions, and types...")
for i in range(33,34):
    print("Extracting from module {}...".format(main_df["module_name"][i]))
    current_module = main_df["module_name"][i]
    current_module_description = main_df["module_description"][i]
    # url to a specific module of the Senegal LSMS
    module_url = url + "/F{}?file_name={}".format(i + 1, current_module)
    print(module_url)
    response = requests.get(module_url)
    soup = BeautifulSoup(response.content, 'html.parser')
    # Search for containers that have the info we want -- which is variable name and link to variable webpage
    var_ids = soup.find_all("a", class_="var-id text-break")
    list_of_dictionaries = []

    # For every variable in the module, extract variable description and variable type by going to the variable webpage
    for i in tqdm(range(len(var_ids))):
        var_link = var_ids[i]["href"]
        var_response = requests.get(var_link)
        var_soup = BeautifulSoup(var_response.content, 'html.parser')
        var_data = var_soup.find_all("div", class_="variable-container")
        # find variable description and data type
        var_description = var_data[0].find("h2").text.strip()
        data_type = var_data[0].find("div", class_="fld-inline sum-stat sum-stat-var_intrvl").text.split(":")[1].strip()
        # translate everything
        list_of_dictionaries.append({"variable_name": var_ids[i].text, "variable_description": var_description, "module_name": current_module, "module_description": current_module_description, "data_type": data_type})

    all_dictionaries += list_of_dictionaries


pd.DataFrame(all_dictionaries).to_csv("~/Downloads/senegal_s18_1.csv", index=False)
