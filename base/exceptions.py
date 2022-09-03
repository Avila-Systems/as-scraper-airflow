class ThresholdException(Exception):
    def __init__(self, threshold: float) -> None:
        self.message = f'Errors passed the {threshold}% threshold'
        super().__init__(self.message)
