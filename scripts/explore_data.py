from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data/handbook-raw/data"


def main():
    files = sorted(DATA_DIR.glob("*.pkl"))

    if not files:
        raise FileNotFoundError(f"No pickle files in {DATA_DIR}")

    print("Daily files:", len(files))
    print("First file:", files[0].name)
    print("Last file:", files[-1].name)

    # The dataset source is trusted for this exercise.
    frames = [pd.read_pickle(path) for path in files]
    df = pd.concat(frames, ignore_index=True)

    print("\nSCHEMA")
    print(df.dtypes)

    print("\nSAMPLE TRANSACTIONS")
    print(df.head().to_string(index=False))

    print("\nDATASET SUMMARY")
    print("Transactions:", len(df))
    print("Unique transaction IDs:", df["TRANSACTION_ID"].nunique())
    print("Customers:", df["CUSTOMER_ID"].nunique())
    print("Terminals:", df["TERMINAL_ID"].nunique())
    print("First transaction:", df["TX_DATETIME"].min())
    print("Last transaction:", df["TX_DATETIME"].max())

    print("\nFRAUD DISTRIBUTION")
    print("Fraudulent transactions:", int(df["TX_FRAUD"].sum()))
    print(f"Fraud rate: {df['TX_FRAUD'].mean():.4%}")

    print("\nTRANSACTION AMOUNT")
    print(df["TX_AMOUNT"].describe())

    print("\nMISSING VALUES")
    print(df.isna().sum())

    print("\nMONTHLY FRAUD RATE")
    df["month"] = df["TX_DATETIME"].dt.to_period("M")
    monthly = df.groupby("month")["TX_FRAUD"].agg(
        transactions="count",
        frauds="sum",
        fraud_rate="mean",
    )
    print(monthly)


if __name__ == "__main__":
    main()
