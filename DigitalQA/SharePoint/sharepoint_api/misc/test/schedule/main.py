import pandas as pd
import numpy as np
import time
import os


def main():
    df = pd.DataFrame.from_dict(data)
    df.to_excel(filepath, index=False)

if __name__ == '__main__':
    ts = int(time.time())
    filepath = os.path.join(os.getcwd(), "outputs", f"data_{ts}.xlsx")

    data = {
        'x': np.random.randint(0, 100, 100),
        'y': np.random.randint(0, 100, 100),
        'z': np.random.randint(0, 100, 100)
    }

    main()
