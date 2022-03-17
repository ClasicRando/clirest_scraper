
def print_progress_bar(iteration: int,
                       total: int,
                       prefix: str = '',
                       suffix: str = '',
                       decimals: int = 1,
                       length: int = 100,
                       fill: str = '█',
                       print_end: str = "\r") -> None:
    """
    Call in a loop to create terminal progress bar

    Parameters
    ----------
    iteration : Int
        current iteration
    total : int
        total iterations
    prefix : str
        prefix string
    suffix : str
        suffix string
    decimals : int
        positive number of decimals in percent complete
    length : int
        character length of bar
    fill : str
        bar fill character
    print_end : str
        end character (e.g. "\\r", "\\r\\n")
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print(f'\r{prefix} |{bar}| {percent}% {suffix}', end=print_end)
    if iteration == total:
        print()
