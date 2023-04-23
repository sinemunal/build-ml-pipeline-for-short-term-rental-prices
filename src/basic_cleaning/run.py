#!/usr/bin/env python
"""
"Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact"
"""
import argparse
import logging
import pandas as pd
import wandb


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Download input artifact. This will also log that this script is using this
    # particular version of the artifact
    logging.info(f"Download the input artifact {args.input_artifact}")
    artifact_local_path = run.use_artifact(args.input_artifact).file()

    logging.info("Read raw data")
    raw_df = pd.read_csv(artifact_local_path)

    logging.info("Begin data cleaning")
    logging.info("Drop outliers")
    idx = raw_df['price'].between(args.min_price, args.max_price)
    df = raw_df[idx].copy()

    logging.info("Convert last_review to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])
    logging.info("Save clean data")
    df.to_csv("clean_sample.csv", index=False)

    logging.info("Upload clean data to W&B")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file("clean_sample.csv")
    run.log_artifact(artifact)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Basic data cleaning")


    parser.add_argument(
        "--input_artifact", 
        type=str,
        help="Raw data as csv",
        required=True
    )

    parser.add_argument(
        "--output_artifact", 
        type=str,
        help="Cleaned data as csv",
        required=True
    )

    parser.add_argument(
        "--output_type", 
        type=str,
        help="Type of the output",
        required=True
    )

    parser.add_argument(
        "--output_description", 
        type=str,
        help="Cleaned data",
        required=True
    )

    parser.add_argument(
        "--min_price", 
        type=int,
        help="minimum price to cap",
        required=True
    )

    parser.add_argument(
        "--max_price", 
        type=int,
        help="maximum price to cap",
        required=True
    )


    args = parser.parse_args()

    go(args)
