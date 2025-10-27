import json
import re
import pandas as pd

from typing import Union, List


def parse(filename: str, columns_to_keep: List[str]) -> Union[pd.DataFrame, None]:
    with open(filename, "r", encoding="utf-8") as file:
        content = file.read().split("--- SEPARATOR ---")[2]

    match = re.search(r"\[2,\s*\[\s*(\{.*?\})\s*\]\s*\]", content, re.DOTALL)

    if match:
        json_part_string = match.group(1)
        try:
            data = json.loads(json_part_string)

            articles_firestore_map = (
                data.get("documentChange", {})
                .get("document", {})
                .get("fields", {})
                .get("articles", {})
                .get("mapValue", {})
                .get("fields", {})
            )

            simplified_articles = {}

            for article_id, article_data in articles_firestore_map.items():
                if "mapValue" in article_data and "fields" in article_data["mapValue"]:
                    simplified_article_fields = {}
                    for field_name, field_value_dict in article_data["mapValue"][
                        "fields"
                    ].items():
                        simplified_article_fields[field_name] = firestore_to_json(
                            field_value_dict
                        )
                    simplified_articles[article_id] = simplified_article_fields

            final_json_output = json.dumps(
                simplified_articles, indent=2, ensure_ascii=False
            )
            df = pd.DataFrame(simplified_articles).T
            df = df[df["article_type"] == 1]
            df = df.reindex(columns=columns_to_keep)
            df = rename_df(df)
            df = df.reset_index(drop=True)
            with open(
                f"db/{filename.split('.')[0].split('/')[-1]}.json",
                "w",
                encoding="utf-8",
            ) as out:
                out.write(final_json_output)

            return df

        except json.JSONDecodeError as e:
            print(f"Error decoding JSON part: {e}")
        except KeyError as e:
            print(f"Error navigating the data structure, missing key: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    else:
        print(
            "Could not find the relevant data structure '[2, [{...}]]' in the input string."
        )


def firestore_to_json(value_dict):
    if not isinstance(value_dict, dict):
        return value_dict

    for key, value in value_dict.items():
        if key == "stringValue":
            return value
        elif key == "integerValue":
            return int(value)
        elif key == "doubleValue":
            return float(value)
        elif key == "booleanValue":
            return value
        elif key == "nullValue":
            return None
        elif key == "timestampValue":
            return value
        elif key == "mapValue":
            if "fields" in value:
                return {
                    f_key: firestore_to_json(f_val)
                    for f_key, f_val in value["fields"].items()
                }
            else:
                return {}
        elif key == "arrayValue":
            if "values" in value:
                return [firestore_to_json(item) for item in value["values"]]
            else:
                return []
        else:
            return {key: value}
    return None


def rename_df(df: pd.DataFrame) -> pd.DataFrame:
    columns = df.columns.tolist()
    if "price_out" in columns:
        df = df.rename(columns={"price_out": "Price"})
    if "format" in columns:
        df = df.rename(columns={"format": "Volume"})
    for c in columns:
        df = df.rename(columns={c: c.capitalize()})
    return df
