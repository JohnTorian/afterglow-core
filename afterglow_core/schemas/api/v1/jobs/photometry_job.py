"""
Afterglow Core: batch photometry job schemas
"""

from typing import List as TList

from marshmallow.fields import Integer, List, Nested

from ..job import JobSchema, JobResultSchema
from ..photometry import PhotSettingsSchema, PhotometryDataSchema
from ..source_extraction import SourceExtractionDataSchema


__all__ = ['PhotometryJobResultSchema', 'PhotometryJobSchema']


class PhotometryJobResultSchema(JobResultSchema):
    data: TList[PhotometryDataSchema] = List(
        Nested(PhotometryDataSchema), default=[])


class PhotometryJobSchema(JobSchema):
    type = 'photometry'

    result: PhotometryJobResultSchema = Nested(
        PhotometryJobResultSchema, default={})
    file_ids: TList[int] = List(Integer(), default=[])
    sources: TList[SourceExtractionDataSchema] = List(
        Nested(SourceExtractionDataSchema), default=[])
    settings: PhotSettingsSchema = Nested(PhotSettingsSchema, default={})
