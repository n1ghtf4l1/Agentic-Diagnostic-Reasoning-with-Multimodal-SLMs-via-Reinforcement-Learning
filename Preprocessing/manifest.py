import pandas as pd, ast
import random

def read_csv(name: str) -> pd.DataFrame:
    return pd.read_csv(name)

def build_manifest():
    cxr_records = pd.read_csv("../Data/cxr-record-list.csv")
    cxr_studies = pd.read_csv("../Data/cxr-study-list.csv")
    print(cxr_records.head())
    print(cxr_records.columns)
    print(cxr_studies.head())
    print(cxr_studies.columns)

    image_by_study = (cxr_records.groupby("study_id")["path"].apply(list).reset_index(name = "image_paths"))
    merged_records = pd.merge(cxr_studies, image_by_study, on="study_id", how="inner")

    print("Total studies:", len(merged_records))
    print(merged_records.head())

    merged_records.to_csv("../Data/study_manifest.csv", index=False)
    print("Save study_manifest.csv...")

    return merged_records

def build_label_subset(label: str, N: int = 800) -> pd.DataFrame:
    split = pd.read_csv("../Data/mimic-cxr-2.0.0-split.csv")

    # get all train study ids as a list
    labeled_id = split[split["split"] == label]['study_id'].tolist()
    
    # ramdomly sample N study ids 
    random.seed(42)
    subset = random.sample(labeled_id, N)

    return pd.DataFrame({"study_id": subset})

def extract_dicom(filename: str):
    df = pd.read_csv(filename)
    dicom_list = []
    for v in df["image_paths"]:
        dicoms = ast.literal_eval(v)
        dicom_list.extend(dicoms)

    dicom_list = sorted(set(dicom_list))

    with open("dicom_list.txt","w") as f:
        for p in dicom_list:
            f.write(p + "\n")

    print("Wrote dicom_list.txt with", len(dicom_list), "files")
    

def main():
    manifest = build_manifest()
    train_subset = build_label_subset("train", N=800)
    val_subset   = build_label_subset("validate", N=100)
    test_subset  = build_label_subset("test", N=100)
    
    train_subset["split"] = "train"
    val_subset["split"] = "validate"
    test_subset["split"] = "test"

    subset_ids = pd.concat([train_subset, val_subset, test_subset], ignore_index=True)

    # save subset manifest(train only)
    train_manifest = pd.merge(manifest, subset_ids, on="study_id", how="inner")
    train_manifest.to_csv("subset_study_manifest.csv", index=False)

    # extract dicom and report paths ( for downloading) 
    extract_dicom("subset_study_manifest.csv")

if __name__ == "__main__":
    main()