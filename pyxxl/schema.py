from dataclasses import asdict, dataclass, fields
from typing import Dict, Optional


@dataclass(frozen=True)
class RunData:
    """
    调度器传入的所有参数，执行函数通过g来获取这些参数

    !!! example

        ```python
        from pyxxl.ctx import g

        @xxxxx
        async def test():
            print(g.xxl_run_data.logId)
        ```

    """

    jobId: int
    logId: int
    executorHandler: str
    executorBlockStrategy: str

    executorParams: Optional[str] = None
    executorTimeout: Optional[int] = None
    logDateTime: Optional[int] = None
    glueType: Optional[str] = None
    glueSource: Optional[str] = None
    glueUpdatetime: Optional[int] = None
    broadcastIndex: Optional[int] = None
    broadcastTotal: Optional[int] = None

    @classmethod
    def from_dict(cls, data: dict) -> "RunData":
        class_fields = {f.name for f in fields(cls)}
        return RunData(**{k: v for k, v in data.items() if k in class_fields})
    
    def to_dict(self) -> Dict[str, any]:
        """Convert RunData to dictionary for serialization."""
        return asdict(self)
