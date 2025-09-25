import src.Source as source
import src.Injest as ingest
import src.Transform as transform



def main():
    # Create Source
    src = source.Source()
    data = src.generate()

    # Injest data
    injest = ingest.Injest(data)
    injested_data = injest.process_data()

    # Transform data
    transform_data = transform.Transform(injested_data)
    final_data = transform_data.transform_data()

    print(final_data)