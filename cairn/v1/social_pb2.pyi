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

class ProjectedActivity(_message.Message):
    __slots__ = ("id", "title", "type", "discipline", "start_time", "timezone", "elapsed_duration_s", "moving_duration_s", "distance_m", "elevation_gain_m", "description", "tss", "calories", "start_place", "start", "start_location_redacted", "avg_hr", "max_hr", "avg_power", "max_power", "normalized_power", "avg_speed_mps", "max_speed_mps", "map_image", "has_photos", "visible_categories")
    ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    DISCIPLINE_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    TIMEZONE_FIELD_NUMBER: _ClassVar[int]
    ELAPSED_DURATION_S_FIELD_NUMBER: _ClassVar[int]
    MOVING_DURATION_S_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_M_FIELD_NUMBER: _ClassVar[int]
    ELEVATION_GAIN_M_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    TSS_FIELD_NUMBER: _ClassVar[int]
    CALORIES_FIELD_NUMBER: _ClassVar[int]
    START_PLACE_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    START_LOCATION_REDACTED_FIELD_NUMBER: _ClassVar[int]
    AVG_HR_FIELD_NUMBER: _ClassVar[int]
    MAX_HR_FIELD_NUMBER: _ClassVar[int]
    AVG_POWER_FIELD_NUMBER: _ClassVar[int]
    MAX_POWER_FIELD_NUMBER: _ClassVar[int]
    NORMALIZED_POWER_FIELD_NUMBER: _ClassVar[int]
    AVG_SPEED_MPS_FIELD_NUMBER: _ClassVar[int]
    MAX_SPEED_MPS_FIELD_NUMBER: _ClassVar[int]
    MAP_IMAGE_FIELD_NUMBER: _ClassVar[int]
    HAS_PHOTOS_FIELD_NUMBER: _ClassVar[int]
    VISIBLE_CATEGORIES_FIELD_NUMBER: _ClassVar[int]
    id: str
    title: str
    type: _activity_pb2.ActivityType
    discipline: str
    start_time: _timestamp_pb2.Timestamp
    timezone: str
    elapsed_duration_s: int
    moving_duration_s: int
    distance_m: float
    elevation_gain_m: float
    description: str
    tss: float
    calories: int
    start_place: str
    start: _common_pb2.GeoPoint
    start_location_redacted: bool
    avg_hr: int
    max_hr: int
    avg_power: int
    max_power: int
    normalized_power: int
    avg_speed_mps: float
    max_speed_mps: float
    map_image: _common_pb2.ContentUrl
    has_photos: bool
    visible_categories: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[str] = ..., title: _Optional[str] = ..., type: _Optional[_Union[_activity_pb2.ActivityType, str]] = ..., discipline: _Optional[str] = ..., start_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., timezone: _Optional[str] = ..., elapsed_duration_s: _Optional[int] = ..., moving_duration_s: _Optional[int] = ..., distance_m: _Optional[float] = ..., elevation_gain_m: _Optional[float] = ..., description: _Optional[str] = ..., tss: _Optional[float] = ..., calories: _Optional[int] = ..., start_place: _Optional[str] = ..., start: _Optional[_Union[_common_pb2.GeoPoint, _Mapping]] = ..., start_location_redacted: _Optional[bool] = ..., avg_hr: _Optional[int] = ..., max_hr: _Optional[int] = ..., avg_power: _Optional[int] = ..., max_power: _Optional[int] = ..., normalized_power: _Optional[int] = ..., avg_speed_mps: _Optional[float] = ..., max_speed_mps: _Optional[float] = ..., map_image: _Optional[_Union[_common_pb2.ContentUrl, _Mapping]] = ..., has_photos: _Optional[bool] = ..., visible_categories: _Optional[_Iterable[str]] = ...) -> None: ...

class FederatedActivity(_message.Message):
    __slots__ = ("id", "title", "sport", "start_time", "summary", "url", "map_image", "distance_m", "elevation_gain_m", "elapsed_duration_s")
    ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    SPORT_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    SUMMARY_FIELD_NUMBER: _ClassVar[int]
    URL_FIELD_NUMBER: _ClassVar[int]
    MAP_IMAGE_FIELD_NUMBER: _ClassVar[int]
    DISTANCE_M_FIELD_NUMBER: _ClassVar[int]
    ELEVATION_GAIN_M_FIELD_NUMBER: _ClassVar[int]
    ELAPSED_DURATION_S_FIELD_NUMBER: _ClassVar[int]
    id: str
    title: str
    sport: str
    start_time: _timestamp_pb2.Timestamp
    summary: str
    url: str
    map_image: _common_pb2.ContentUrl
    distance_m: float
    elevation_gain_m: float
    elapsed_duration_s: int
    def __init__(self, id: _Optional[str] = ..., title: _Optional[str] = ..., sport: _Optional[str] = ..., start_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., summary: _Optional[str] = ..., url: _Optional[str] = ..., map_image: _Optional[_Union[_common_pb2.ContentUrl, _Mapping]] = ..., distance_m: _Optional[float] = ..., elevation_gain_m: _Optional[float] = ..., elapsed_duration_s: _Optional[int] = ...) -> None: ...

class FeedItem(_message.Message):
    __slots__ = ("source", "owner", "own", "projected", "federated")
    class Source(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = ()
        SOURCE_UNSPECIFIED: _ClassVar[FeedItem.Source]
        SOURCE_SELF: _ClassVar[FeedItem.Source]
        SOURCE_FOLLOWING: _ClassVar[FeedItem.Source]
        SOURCE_FEDERATED: _ClassVar[FeedItem.Source]
    SOURCE_UNSPECIFIED: FeedItem.Source
    SOURCE_SELF: FeedItem.Source
    SOURCE_FOLLOWING: FeedItem.Source
    SOURCE_FEDERATED: FeedItem.Source
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    OWN_FIELD_NUMBER: _ClassVar[int]
    PROJECTED_FIELD_NUMBER: _ClassVar[int]
    FEDERATED_FIELD_NUMBER: _ClassVar[int]
    source: FeedItem.Source
    owner: _common_pb2.UserCard
    own: _activity_pb2.Activity
    projected: ProjectedActivity
    federated: FederatedActivity
    def __init__(self, source: _Optional[_Union[FeedItem.Source, str]] = ..., owner: _Optional[_Union[_common_pb2.UserCard, _Mapping]] = ..., own: _Optional[_Union[_activity_pb2.Activity, _Mapping]] = ..., projected: _Optional[_Union[ProjectedActivity, _Mapping]] = ..., federated: _Optional[_Union[FederatedActivity, _Mapping]] = ...) -> None: ...

class ListHomeFeedRequest(_message.Message):
    __slots__ = ("page",)
    PAGE_FIELD_NUMBER: _ClassVar[int]
    page: _common_pb2.PageRequest
    def __init__(self, page: _Optional[_Union[_common_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListHomeFeedResponse(_message.Message):
    __slots__ = ("items", "page")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[FeedItem]
    page: _common_pb2.PageResponse
    def __init__(self, items: _Optional[_Iterable[_Union[FeedItem, _Mapping]]] = ..., page: _Optional[_Union[_common_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class ListFollowingFeedRequest(_message.Message):
    __slots__ = ("page",)
    PAGE_FIELD_NUMBER: _ClassVar[int]
    page: _common_pb2.PageRequest
    def __init__(self, page: _Optional[_Union[_common_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListFollowingFeedResponse(_message.Message):
    __slots__ = ("items", "page")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[FeedItem]
    page: _common_pb2.PageResponse
    def __init__(self, items: _Optional[_Iterable[_Union[FeedItem, _Mapping]]] = ..., page: _Optional[_Union[_common_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class ClubCard(_message.Message):
    __slots__ = ("id", "slug", "name", "is_public", "member_count", "created_at")
    ID_FIELD_NUMBER: _ClassVar[int]
    SLUG_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    IS_PUBLIC_FIELD_NUMBER: _ClassVar[int]
    MEMBER_COUNT_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    id: str
    slug: str
    name: str
    is_public: bool
    member_count: int
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, id: _Optional[str] = ..., slug: _Optional[str] = ..., name: _Optional[str] = ..., is_public: _Optional[bool] = ..., member_count: _Optional[int] = ..., created_at: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class ListClubsRequest(_message.Message):
    __slots__ = ("page",)
    PAGE_FIELD_NUMBER: _ClassVar[int]
    page: _common_pb2.PageRequest
    def __init__(self, page: _Optional[_Union[_common_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListClubsResponse(_message.Message):
    __slots__ = ("clubs", "page")
    CLUBS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    clubs: _containers.RepeatedCompositeFieldContainer[ClubCard]
    page: _common_pb2.PageResponse
    def __init__(self, clubs: _Optional[_Iterable[_Union[ClubCard, _Mapping]]] = ..., page: _Optional[_Union[_common_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class GetClubRequest(_message.Message):
    __slots__ = ("slug",)
    SLUG_FIELD_NUMBER: _ClassVar[int]
    slug: str
    def __init__(self, slug: _Optional[str] = ...) -> None: ...

class GetClubResponse(_message.Message):
    __slots__ = ("club", "description", "is_member", "my_role")
    CLUB_FIELD_NUMBER: _ClassVar[int]
    DESCRIPTION_FIELD_NUMBER: _ClassVar[int]
    IS_MEMBER_FIELD_NUMBER: _ClassVar[int]
    MY_ROLE_FIELD_NUMBER: _ClassVar[int]
    club: ClubCard
    description: str
    is_member: bool
    my_role: str
    def __init__(self, club: _Optional[_Union[ClubCard, _Mapping]] = ..., description: _Optional[str] = ..., is_member: _Optional[bool] = ..., my_role: _Optional[str] = ...) -> None: ...

class ListClubMembersRequest(_message.Message):
    __slots__ = ("slug", "page")
    SLUG_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    slug: str
    page: _common_pb2.PageRequest
    def __init__(self, slug: _Optional[str] = ..., page: _Optional[_Union[_common_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ClubMember(_message.Message):
    __slots__ = ("user", "role")
    USER_FIELD_NUMBER: _ClassVar[int]
    ROLE_FIELD_NUMBER: _ClassVar[int]
    user: _common_pb2.UserCard
    role: str
    def __init__(self, user: _Optional[_Union[_common_pb2.UserCard, _Mapping]] = ..., role: _Optional[str] = ...) -> None: ...

class ListClubMembersResponse(_message.Message):
    __slots__ = ("members", "page")
    MEMBERS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    members: _containers.RepeatedCompositeFieldContainer[ClubMember]
    page: _common_pb2.PageResponse
    def __init__(self, members: _Optional[_Iterable[_Union[ClubMember, _Mapping]]] = ..., page: _Optional[_Union[_common_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class ListClubFeedRequest(_message.Message):
    __slots__ = ("slug", "page")
    SLUG_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    slug: str
    page: _common_pb2.PageRequest
    def __init__(self, slug: _Optional[str] = ..., page: _Optional[_Union[_common_pb2.PageRequest, _Mapping]] = ...) -> None: ...

class ListClubFeedResponse(_message.Message):
    __slots__ = ("items", "page")
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    PAGE_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[FeedItem]
    page: _common_pb2.PageResponse
    def __init__(self, items: _Optional[_Iterable[_Union[FeedItem, _Mapping]]] = ..., page: _Optional[_Union[_common_pb2.PageResponse, _Mapping]] = ...) -> None: ...

class GetProfileRequest(_message.Message):
    __slots__ = ("username",)
    USERNAME_FIELD_NUMBER: _ClassVar[int]
    username: str
    def __init__(self, username: _Optional[str] = ...) -> None: ...

class GetProfileResponse(_message.Message):
    __slots__ = ("user", "is_public", "is_self", "is_following", "followers_count", "following_count", "activities")
    USER_FIELD_NUMBER: _ClassVar[int]
    IS_PUBLIC_FIELD_NUMBER: _ClassVar[int]
    IS_SELF_FIELD_NUMBER: _ClassVar[int]
    IS_FOLLOWING_FIELD_NUMBER: _ClassVar[int]
    FOLLOWERS_COUNT_FIELD_NUMBER: _ClassVar[int]
    FOLLOWING_COUNT_FIELD_NUMBER: _ClassVar[int]
    ACTIVITIES_FIELD_NUMBER: _ClassVar[int]
    user: _common_pb2.UserCard
    is_public: bool
    is_self: bool
    is_following: bool
    followers_count: int
    following_count: int
    activities: _containers.RepeatedCompositeFieldContainer[ProjectedActivity]
    def __init__(self, user: _Optional[_Union[_common_pb2.UserCard, _Mapping]] = ..., is_public: _Optional[bool] = ..., is_self: _Optional[bool] = ..., is_following: _Optional[bool] = ..., followers_count: _Optional[int] = ..., following_count: _Optional[int] = ..., activities: _Optional[_Iterable[_Union[ProjectedActivity, _Mapping]]] = ...) -> None: ...

class GetProjectedActivityRequest(_message.Message):
    __slots__ = ("activity_id",)
    ACTIVITY_ID_FIELD_NUMBER: _ClassVar[int]
    activity_id: str
    def __init__(self, activity_id: _Optional[str] = ...) -> None: ...

class GetProjectedActivityResponse(_message.Message):
    __slots__ = ("activity", "owner", "is_owner")
    ACTIVITY_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    IS_OWNER_FIELD_NUMBER: _ClassVar[int]
    activity: ProjectedActivity
    owner: _common_pb2.UserCard
    is_owner: bool
    def __init__(self, activity: _Optional[_Union[ProjectedActivity, _Mapping]] = ..., owner: _Optional[_Union[_common_pb2.UserCard, _Mapping]] = ..., is_owner: _Optional[bool] = ...) -> None: ...

class GetSharedActivityRequest(_message.Message):
    __slots__ = ("token",)
    TOKEN_FIELD_NUMBER: _ClassVar[int]
    token: str
    def __init__(self, token: _Optional[str] = ...) -> None: ...

class GetSharedActivityResponse(_message.Message):
    __slots__ = ("activity", "owner")
    ACTIVITY_FIELD_NUMBER: _ClassVar[int]
    OWNER_FIELD_NUMBER: _ClassVar[int]
    activity: ProjectedActivity
    owner: _common_pb2.UserCard
    def __init__(self, activity: _Optional[_Union[ProjectedActivity, _Mapping]] = ..., owner: _Optional[_Union[_common_pb2.UserCard, _Mapping]] = ...) -> None: ...

class ListReviewQueueRequest(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ReviewItem(_message.Message):
    __slots__ = ("id", "title", "type", "start_time", "match_confidence", "kind", "duplicate_of_id")
    ID_FIELD_NUMBER: _ClassVar[int]
    TITLE_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    MATCH_CONFIDENCE_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    DUPLICATE_OF_ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    title: str
    type: _activity_pb2.ActivityType
    start_time: _timestamp_pb2.Timestamp
    match_confidence: str
    kind: str
    duplicate_of_id: str
    def __init__(self, id: _Optional[str] = ..., title: _Optional[str] = ..., type: _Optional[_Union[_activity_pb2.ActivityType, str]] = ..., start_time: _Optional[_Union[datetime.datetime, _timestamp_pb2.Timestamp, _Mapping]] = ..., match_confidence: _Optional[str] = ..., kind: _Optional[str] = ..., duplicate_of_id: _Optional[str] = ...) -> None: ...

class ListReviewQueueResponse(_message.Message):
    __slots__ = ("items",)
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    items: _containers.RepeatedCompositeFieldContainer[ReviewItem]
    def __init__(self, items: _Optional[_Iterable[_Union[ReviewItem, _Mapping]]] = ...) -> None: ...
