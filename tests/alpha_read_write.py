import DataPlatForm.AlphaWrite as awrite
import DataPlatForm.AlphaRead as aread
import DataPlatForm.BasicUtils as butils

from clickhouse_driver import Client
import time

if __name__ == "__main__":
    # generate a simulative factor data frame, size: 4000 * 4000
    symbols = butils.gen_symbol_list(4000)
    frame = butils.gen_factor_dataframe(symbols, "2005-01-01", 4000)

    # write frame into clickhouse database
    alpha_name = "test_alpha"
    awrite.write_alphafactor(frame, alpha_name)

    # read full frame from clickhouse database
    start_time = time.time()
    print(aread.read_alphafactor(alpha_name))
    print("full read cost: ", time.time() - start_time)
    print("\n")

    # read frame after a specific date
    print(aread.read_alphafactor(alpha_name, "2015-01-10"))
    print("\n")

    # read frame in a specific date range
    start_time = time.time()
    print(aread.read_alphafactor(alpha_name, "2005-01-10", "2012-02-10"))
    print("sub read cost: ", time.time() - start_time)
    print("\n")

    # delete frame data in a specific date range and read again
    awrite.delete_alphafactor(alpha_name, "2015-01-02", "2016-02-10")
    
    time.sleep(5)
    print(aread.read_alphafactor(alpha_name))
    print("\n")