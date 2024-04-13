import pandas as pd

def read_excel_to_dict(file_path):

    data = pd.read_excel(file_path)
    option_dict = {}

    for _, row in data.iterrows():
        code = row['option_code']
        value = row['option_values'].strip()  

        if code not in option_dict:
            option_dict[code] = [value]
        else:
            if value not in option_dict[code]:
                option_dict[code].append(value)

    return option_dict

if __name__ == "__main__":
    excel_path = 'options.xlsx'  
    try:
        data_dict = read_excel_to_dict(excel_path)
        print("Options dhis2 meta_data:")
        for code, values in data_dict.items():
            print(f"{code}: {values}")
    except Exception as e:
        print(f"An error occurred: {e}")
