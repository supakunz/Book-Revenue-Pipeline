import datetime

def get_filename_with_date(save_path, filename, date=None):
    if date is None:
        basename = f"{save_path}/{filename}"
        date = datetime.datetime.today()
    date_str = date.strftime("%Y%m%d")
    filename = f"{basename}_{date_str}"
    return filename