
from openpyxl.styles import PatternFill, Border, Side, Alignment, Protection, Font

class ExportBase():
    def _red_font(self):
        font = Font(name='Calibri', size=11, bold=False, italic=False, vertAlign=None, underline='none', strike=False,
                    color='FFFF0000')
        return font