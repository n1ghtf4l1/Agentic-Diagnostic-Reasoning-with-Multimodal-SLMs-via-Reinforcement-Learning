from mimic_dataset_vlm import MIMICImpressionDataset

ds = MIMICImpressionDataset("mimic_impression_subset.csv", root="../Data/Images")
sample = ds[0]

print("Study:", sample["study_id"])
print("Reference Impression:", sample["reference"])
sample["image"].show()