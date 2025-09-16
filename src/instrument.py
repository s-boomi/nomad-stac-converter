from pystac.extensions.eo import Band


class Nomad:
    def __init__(self):
        self.bands = {
            "so": Band.create(
                name="SO",
                common_name="so",
                description="Solar Occultation, 2.2–4.3 μm",
                full_width_half_max=4.3 - 2.2,
                center_wavelength=(4.3 + 2.2) / 2,
            ),
            "lno": Band.create(
                name="LNO",
                common_name="lno",
                description="Limb, Nadir and Occultation, 2.2–3.8 μm",
                full_width_half_max=3.8 - 2.2,
                center_wavelength=(3.8 + 2.2) / 2,
            ),
            "uvis": Band.create(
                name="UVIS",
                common_name="uvis",
                description="Ultraviolet–VISible, 200-650 nm",
                full_width_half_max=(650 - 200) * 10e-3,
                center_wavelength=((650 + 200) / 2) * 10e-3,
            ),
        }
