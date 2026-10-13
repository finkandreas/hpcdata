from typing import cast, TypedDict, Optional, Sequence, Union
from datetime import datetime

import requests

class Config(TypedDict):
    base_url: str
    client_id: str
    client_secret: str
    token_url: str
    jobid: str
    cluster: str

def generate_token(config: Config) -> str:

    basic = requests.auth.HTTPBasicAuth(config['client_id'], config['client_secret'])
    r = requests.post(config['token_url'], data={'grant_type': 'client_credentials'}, auth=basic)
    r.raise_for_status()
    return cast(str, r.json()['access_token'])

def make_format(current, other): # type: ignore
    # current and other are axes
    def format_coord(x, y): # type: ignore
        # x, y are data coordinates
        # convert to display coords
        date = current.format_xdata(x)
        display_coord = current.transData.transform((x,y))
        inv = other.transData.inverted()
        # convert back to data coords with respect to ax
        ax_coord = inv.transform(display_coord)
        coords = [ax_coord, (x, y)]
        return f'{date} funds={ax_coord[1]:1.0f} profit={y:1.0f}'
    return format_coord

def _to_float_time(t: Union[float, int, datetime]) -> float:
    if isinstance(t, datetime):
        return t.timestamp()
    return float(t)

def interpolate(
    y: Sequence[Union[int, float]],
    x: Optional[Sequence[Union[int, float, datetime]]] = None,
    max_hole_size: int = 0
) -> list[Union[float, int]]:
    """
    Replace data holes (zeros) between two non-zero values with linearly
    interpolated values.

    If a data hole is bigger than the input max_hole_size, leave the hole as is.

    Parameters
    ----------
    y:
        Input data, potentially with holes
    x: [Optional]
        Sequence of x values (same length as y).
        If omitted, x values are assumed equally spaced.
    max_hole_size:
        Maximum number of consecutive zeros to interpolate across.
        0 means no limit.
    """
    result = [float(u) for u in y]
    n = len(result)
    
    if n < 3:
        return result

    if x is not None:
        if len(x) != n:
            raise ValueError("x and y must have the same length.")
        t = [_to_float_time(u) for u in x]
    else:
        t = list(range(n))

    i = 0
    while i < n:
        if result[i] != 0:
            i += 1
            continue

        start = i
        while ((i < n) and (result[i] == 0)):
            i += 1
        end = i

        if start == 0 or end >= n:
            # can't interpolate when boundary values are missing
            continue

        gap = end - start
        if max_hole_size > 0 and gap > max_hole_size:
            # don't interpolate on holes bigger than max_hole_size
            continue

        v_left = result[start - 1]
        v_right = result[end]
        t_left = t[start - 1]
        t_right = t[end]

        if t_right == t_left:
            raise ValueError("found duplicate x (time) values")

        slope = (v_right - v_left) / (t_right - t_left)
        for j in range(start, end):
            result[j] = v_left + slope * (t[j] - t_left)

    return result