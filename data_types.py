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

print("Extracting module names...")
df["module_name"] = df["Data file"].apply(lambda x: x.split()[0])

# get rid of the Data File column in df because it is separated into module name and description now.
df.drop(columns=["Data file"], inplace=True)
main_df = df.copy()
print(main_df["module_name"])

# For every module in LSMS, extract variable_name, variable_description, data_type, and unique_id
all_dictionaries = []
print("Extracting variable names, descriptions, and types...")
for i in range(len(main_df)):
    print("Extracting from module {}...".format(main_df["module_name"][i]))
    current_module = main_df["module_name"][i]
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
        #print(var_data)
        # find variable description and data type
        #data_type = var_data[0].find("div", class_="field-value").text.split(":")[1].strip()

        data_type = None
        tracker = None
        counter = 0
        while (data_type is None) and (tracker != "Done"):
            for element in var_data:
                if counter == 2:
                    data_type = "categorical"
                elif ("<th>Category</th>" in str(element)) or ("<th>Cases</th>" in str(element)):
                    counter += 1
                elif element == var_data[-1]:
                    tracker = "Done"
                    data_type = "inconclusive"

        if data_type == "inconclusive":
            data_type = var_data[0].find("div", class_="fld-inline sum-stat var-format").text.split(":")[1].strip()
            if data_type == "Numeric":
                data_type = "numeric"
            elif data_type == "character":
                data_type = "string"

        print(data_type)

        list_of_dictionaries.append({"variable_name": var_ids[i].text, "module_name": current_module, "data_type": data_type})
    print(list_of_dictionaries)
    all_dictionaries += list_of_dictionaries


pd.DataFrame(all_dictionaries).to_csv("~/Downloads/senegal_datatype_list.csv", index=False)