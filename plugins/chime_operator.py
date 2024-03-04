from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from chime import Chime

class ChimeOperator(BaseOperator):
    """
    Plays a sound notification using the Chime library.
    """
    @apply_defaults
    def __init__(self, sound='success', **kwargs):
        """
        :param sound: The type of sound to play. Options are 'success' or 'failure'.
        :type sound: str
        """
        super().__init__(**kwargs)
        self.sound = sound

    def execute(self, context):
        if self.sound == 'success':
            Chime.success()
        elif self.sound == 'failure':
            Chime.failure()
