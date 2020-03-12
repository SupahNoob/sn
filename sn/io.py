from typing import Union
import pathlib
import csv


def spss_value_encoder(v: Union[None, float, str]) -> Union[None, float, str]:
    """
    Encodes values found in an SPSS file.
    
    Parameters
    ----------
    v : None or float or str
        value to convert
    
    Returns
    -------
    transformed_v
    """
    if v is None:
        return v

    # duck typing a numeric.. to see if we should convert CP1252 --> UTF-8
    try:
        v + 0
    except TypeError:
        v = v.decode('CP1252')

    return v
    

def spss_to_csv(fp: pathlib.Path) -> None:
    """
    Converts an SPSS SAV file to CSV.
    
    The encoding format will be CP1252 for all string helds in the CSV file. The
    CSV file will be saved to the same directory as the input SPSS SAV file,
    with a different extension (CSV, naturally).

    Parameters
    ----------
    fp : str
        location on disk where the SPSS sav file is held
        
    Returns
    -------
    None
    """
    from savReaderWriter import SavReader


    with SavReader(fp) as sav:
        r, c = sav.shape.nrows, sav.shape.ncols
        print(f'shape: ({r}, {c})')

        with (fp.parent / f'{fp.stem}.csv').open('w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([c.decode('CP1252') for c in sav.header])

            for line in sav:
                writer.writerow(list(map(spss_value_encoder, line)))
