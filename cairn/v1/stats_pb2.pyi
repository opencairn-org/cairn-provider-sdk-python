import datetime

from cairn.v1 import activity_pb2 as _activity_pb2
from cairn.v1 import common_pb2 as _common_pb2
from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetOverviewRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class GetOverviewResponse(_message.Message):
    __slots__ = ("totals", "by_sport", "recent", "this_week", "last_week", "this_week_range", "last_week_range")
    TOTALS_FIELD_NUMBER: _ClassVar[int]
    BY_SPORT_FIELD_NUMBER: _ClassVar[int]
    RECENT_FIELD_NUMBER: _ClassVar[int]
    THIS_WEEK_FIELD_NUMBER: _ClassVar[int]
    LAST_WEEK_FIELD_NUMBER: _ClassVar[int]
    THIS_WEEK_RANGE_FIELD_NUMBER: _ClassVar[int]
    LAST_WEEK_RANGE_FIELD_NUMBER: _ClassVar[int]
    totals: _common_pb2.Totals
    by_sport: _containers.RepeatedCompositeFieldContainer[_common_pb2.Facet]
    recent: _containers.RepeatedCompositeFieldContainer[_activity_pb2.Activity]
    this_week: _common_pb2.Totals
    last_week: _common_pb2.Totals
    this_week_range: _common_pb2.TimeRange
    last_week_range: _common_pb2.TimeRange
    def __init__(self, totals: _Optional[_Union[_common_pb2.Totals, _Mapping]] = ..., by_sport: _Optional[_Iterable[_Union[_common_pb2.Facet, _Mapping]]] = ..., recent: _Optional[_Iterable[_Union[_activity_pb2.Activity, _Mapping]]] = ..., this_week: _Optional[_Union[_common_pb2.Totals, _Mapping]] = ..., last_week: _Optional[_Union[_common_pb2.Totals, _Mapping]] = ..., this_week_range: _Optional[_Union[_common_pb2.TimeRange, _Mapping]] = ..., last_week_range: _Optional[_Union[_common_pb2.TimeRange, _Mapping]] = ...) -> None: ...

class GetStatsRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class YearStat(_message.Message):
    __slots__ = ("year", "count", "distance_m", "moving_s", "elevation_gain_m")
    YEAR_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_M_FIELD_NUMBER: _ClassVar[int]
    MOVING_S_FIELD_NUMBER: _ClassVar[int]
    ELEVATION_GAIN_M_FIELD_NUMBER: _ClassVar[int]
    year: int
    count: int
    distance_m: float
    moving_s: float
    elevation_gain_m: float
    def __init__(self, year: _Optional[int] = ..., count: _Optional[int] = ..., distance_m: _Optional[float] = ..., moving_s: _Optional[float] = ..., elevation_gain_m: _Optional[float] = ...) -> None: ...

class GetStatsResponse(_message.Message):
    __slots__ = ("totals", "by_sport", "by_discipline", "by_year", "longest_distance", "longest_duration")
    TOTALS_FIELD_NUMBER: _ClassVar[int]
    BY_SPORT_FIELD_NUMBER: _ClassVar[int]
    BY_DISCIPLINE_FIELD_NUMBER: _ClassVar[int]
    BY_YEAR_FIELD_NUMBER: _ClassVar[int]
    LONGEST_DISTANCE_FIELD_NUMBER: _ClassVar[int]
    LONGEST_DURATION_FIELD_NUMBER: _ClassVar[int]
    totals: _common_pb2.Totals
    by_sport: _containers.RepeatedCompositeFieldContainer[_common_pb2.Facet]
    by_discipline: _containers.RepeatedCompositeFieldContainer[_common_pb2.Facet]
    by_year: _containers.RepeatedCompositeFieldContainer[YearStat]
    longest_distance: _activity_pb2.Activity
    longest_duration: _activity_pb2.Activity
    def __init__(self, totals: _Optional[_Union[_common_pb2.Totals, _Mapping]] = ..., by_sport: _Optional[_Iterable[_Union[_common_pb2.Facet, _Mapping]]] = ..., by_discipline: _Optional[_Iterable[_Union[_common_pb2.Facet, _Mapping]]] = ..., by_year: _Optional[_Iterable[_Union[YearStat, _Mapping]]] = ..., longest_distance: _Optional[_Union[_activity_pb2.Activity, _Mapping]] = ..., longest_duration: _Optional[_Union[_activity_pb2.Activity, _Mapping]] = ...) -> None: ...

class GetTrainingLoadRequest(_message.Message):
    __slots__ = ("days",)
    DAYS_FIELD_NUMBER: _ClassVar[int]
    days: int
    def __init__(self, days: _Optional[int] = ...) -> None: ...

class TrainingLoadDay(_message.Message):
    __slots__ = ("date", "ctl", "atl", "tsb", "tss")
    DATE_FIELD_NUMBER: _ClassVar[int]
    CTL_FIELD_NUMBER: _ClassVar[int]
    ATL_FIELD_NUMBER: _ClassVar[int]
    TSB_FIELD_NUMBER: _ClassVar[int]
    TSS_FIELD_NUMBER: _ClassVar[int]
    date: str
    ctl: float
    atl: float
    tsb: float
    tss: float
    def __init__(self, date: _Optional[str] = ..., ctl: _Optional[float] = ..., atl: _Optional[float] = ..., tsb: _Optional[float] = ..., tss: _Optional[float] = ...) -> None: ...

class GetTrainingLoadResponse(_message.Message):
    __slots__ = ("days", "current_ctl", "current_atl", "current_tsb")
    DAYS_FIELD_NUMBER: _ClassVar[int]
    CURRENT_CTL_FIELD_NUMBER: _ClassVar[int]
    CURRENT_ATL_FIELD_NUMBER: _ClassVar[int]
    CURRENT_TSB_FIELD_NUMBER: _ClassVar[int]
    days: _containers.RepeatedCompositeFieldContainer[TrainingLoadDay]
    current_ctl: float
    current_atl: float
    current_tsb: float
    def __init__(self, days: _Optional[_Iterable[_Union[TrainingLoadDay, _Mapping]]] = ..., current_ctl: _Optional[float] = ..., current_atl: _Optional[float] = ..., current_tsb: _Optional[float] = ...) -> None: ...

class GetBestEffortHistoryRequest(_message.Message):
    __slots__ = ("type", "metric", "window_kind", "window_value")
    TYPE_FIELD_NUMBER: _ClassVar[int]
    METRIC_FIELD_NUMBER: _ClassVar[int]
    WINDOW_KIND_FIELD_NUMBER: _ClassVar[int]
    WINDOW_VALUE_FIELD_NUMBER: _ClassVar[int]
    type: _activity_pb2.ActivityType
    metric: _activity_pb2.BestEffortMetric
    window_kind: _activity_pb2.BestEffortWindowKind
    window_value: int
    def __init__(self, type: _Optional[_Union[_activity_pb2.ActivityType, str]] = ..., metric: _Optional[_Union[_activity_pb2.BestEffortMetric, str]] = ..., window_kind: _Optional[_Union[_activity_pb2.BestEffortWindowKind, str]] = ..., window_value: _Optional[int] = ...) -> None: ...

class BestEffortHistoryItem(_message.Message):
    __slots__ = ("activity_id", "title", "start_time", "achieved_value", "is_best")
    ACTIVITY_ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    ACHIEVED_VALUE_FIELD_NUMBER: _ClassVar[int]
    IS_BEST_FIELD_NUMBER: _ClassVar[int]
    activity_id: str
    title: str
    start_time: _timestamp_pb2.Timestamp
    achieved_value: float
    is_best: bool
    def __init__(self, activity_id: _Optional[str] = ..., title: _Optional[str] = ..., start_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., achieved_value: _Optional[float] = ..., is_best: _Optional[bool] = ...) -> None: ...

class GetBestEffortHistoryResponse(_message.Message):
    __slots__ = ("best", "count", "items")
    BEST_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    best: float
    count: int
    items: _containers.RepeatedCompositeFieldContainer[BestEffortHistoryItem]
    def __init__(self, best: _Optional[float] = ..., count: _Optional[int] = ..., items: _Optional[_Iterable[_Union[BestEffortHistoryItem, _Mapping]]] = ...) -> None: ...

class GetHeatmapRequest(_message.Message):
    __slots__ = ("filter", "resolution")
    class Resolution(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        RESOLUTION_UNSPECIFIED: _ClassVar[GetHeatmapRequest.Resolution]
        RESOLUTION_30S: _ClassVar[GetHeatmapRequest.Resolution]
        RESOLUTION_5S: _ClassVar[GetHeatmapRequest.Resolution]
    RESOLUTION_UNSPECIFIED: GetHeatmapRequest.Resolution
    RESOLUTION_30S: GetHeatmapRequest.Resolution
    RESOLUTION_5S: GetHeatmapRequest.Resolution
    FILTER_FIELD_NUMBER: _ClassVar[int]
    RESOLUTION_FIELD_NUMBER: _ClassVar[int]
    filter: _activity_pb2.ActivityFilter
    resolution: GetHeatmapRequest.Resolution
    def __init__(self, filter: _Optional[_Union[_activity_pb2.ActivityFilter, _Mapping]] = ..., resolution: _Optional[_Union[GetHeatmapRequest.Resolution, str]] = ...) -> None: ...

class HeatmapTrack(_message.Message):
    __slots__ = ("activity_id", "type", "discipline", "polyline")
    ACTIVITY_ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    DISCIPLINE_FIELD_NUMBER: _ClassVar[int]
    POLYLINE_FIELD_NUMBER: _ClassVar[int]
    activity_id: str
    type: _activity_pb2.ActivityType
    discipline: str
    polyline: _common_pb2.EncodedPolyline
    def __init__(self, activity_id: _Optional[str] = ..., type: _Optional[_Union[_activity_pb2.ActivityType, str]] = ..., discipline: _Optional[str] = ..., polyline: _Optional[_Union[_common_pb2.EncodedPolyline, _Mapping]] = ...) -> None: ...

class GetHeatmapResponse(_message.Message):
    __slots__ = ("matched", "with_track", "truncated", "facets", "tracks")
    MATCHED_FIELD_NUMBER: _ClassVar[int]
    WITH_TRACK_FIELD_NUMBER: _ClassVar[int]
    TRUNCATED_FIELD_NUMBER: _ClassVar[int]
    FACETS_FIELD_NUMBER: _ClassVar[int]
    TRACKS_FIELD_NUMBER: _ClassVar[int]
    matched: int
    with_track: int
    truncated: bool
    facets: _activity_pb2.ActivityFacets
    tracks: _containers.RepeatedCompositeFieldContainer[HeatmapTrack]
    def __init__(self, matched: _Optional[int] = ..., with_track: _Optional[int] = ..., truncated: _Optional[bool] = ..., facets: _Optional[_Union[_activity_pb2.ActivityFacets, _Mapping]] = ..., tracks: _Optional[_Iterable[_Union[HeatmapTrack, _Mapping]]] = ...) -> None: ...
