"""Shared expected failures for interactive evidence selection and review."""


class ReviewInputError(ValueError):
    """An expected request failure that an interactive agent can correct."""


class IncompleteGalleryError(ReviewInputError):
    def __init__(
        self,
        unavailable_gallery_orders: tuple[int, ...],
        gallery_absence_reason: str | None = None,
    ):
        self.unavailable_gallery_orders: tuple[int, ...] = unavailable_gallery_orders
        self.gallery_absence_reason: str | None = gallery_absence_reason
        if gallery_absence_reason is not None:
            message = (
                f"Selected listing observation has no usable gallery: {gallery_absence_reason}"
            )
        else:
            positions = ", ".join(str(order) for order in unavailable_gallery_orders)
            message = f"Selected listing observation has unavailable gallery positions: {positions}"
        super().__init__(message)
