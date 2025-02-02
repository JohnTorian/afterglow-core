"""
Afterglow Core: data provider schemas
"""

from datetime import datetime
from typing import Any, Dict as TDict, List as TList, Optional

from marshmallow.fields import Dict, Integer, List, String

from ....models import Session
from ... import Boolean, DateTime, Resource


__all__ = ['DataFileSchema', 'SessionSchema']


class DataFileSchema(Resource):
    """
    JSON-serializable data file class

    Fields:
        id: unique integer data file ID; assigned automatically when creating
            or importing data file into session
        type: "image" or "table"
        name: data file name; on import, set to the data provider asset name
        width: image width or number of table columns
        height: image height or number of table rows
        data_provider: for imported data files, name of the originating data
            provider; not defined for data files created from scratch or
            uploaded
        asset_path: for imported data files, the original asset path
        asset_type: original asset file type ("FITS", "JPEG", etc.)
        asset_metadata: dictionary of the originating data provider asset
            metadata
        layer: layer ID for data files imported from multi-layer data provider
            assets
        created_on: datetime.datetime of data file creation
        modified: True if the file was modified after creation
        modified_on: datetime.datetime of data file modification
        session_id: ID of session if the data file is associated with a session
        group_name: name of the data file group
        group_order: 0-based order of the data file in the group
    """
    __get_view__ = 'data_files'

    id: int = Integer(default=None)
    type: str = String(default=None)
    name: str = String(default=None)
    width: int = Integer(default=None)
    height: int = Integer(default=None)
    data_provider: str = String(default=None)
    asset_path: str = String(default=None)
    asset_type: str = String(default=None)
    asset_metadata: TDict[str, Any] = Dict(default={})
    layer: str = String(default=None)
    created_on: datetime = DateTime(
        default=None, format='%Y-%m-%d %H:%M:%S.%f')
    modified: bool = Boolean(default=False)
    modified_on: datetime = DateTime(
        default=None, format='%Y-%m-%d %H:%M:%S.%f')
    session_id: Optional[int] = Integer(default=None)
    group_name: str = String(default=None)
    group_order: int = Integer(default=0)


class SessionSchema(Resource):
    """
    JSON-serializable Afterglow session class

    A session is a collection of user's data files. When creating or importing
    a data file, it is associated with a certain session (by default, if no
    session ID provided, with the anonymous session that always exists).
    Sessions are created by the user via the /sessions endpoint. Their main
    purpose is to provide independent Afterglow UI workspaces; in addition,
    they may serve as a means to group data files by the client API scripts.

    Fields:
        id: unique integer session ID; assigned automatically when creating
            the session
        name: unique session name
        data: arbitrary user data associated with the session
        data_file_ids: list of data file IDs associated with the session
    """
    __get_view__ = 'sessions'

    id: int = Integer(default=None)
    name: str = String(default=None)
    data: str = String()
    data_file_ids: TList[int] = List(Integer(), default=[], dump_only=True)

    def __init__(self, _obj: Optional[Session] = None, **kwargs):
        """
        Create a session schema from a session model (output) or request
        arguments (input)

        :param _obj: session model (used on output)
        :param kwargs: initialize fields from keyword=value pairs (input)
        """
        if _obj is not None:
            # Extract data file IDs if creating from a model
            kwargs['data_file_ids'] = [data_file.id
                                       for data_file in _obj.data_files]

        super().__init__(_obj, **kwargs)
