import unittest

from formatting import markdown_to_html


class FormattingTests(unittest.TestCase):
    def test_bold_conversion(self):
        self.assertEqual(markdown_to_html('**текст**'), '<b>текст</b>')

    def test_newline_not_br(self):
        result = markdown_to_html('строка1\nстрока2')
        self.assertIn('\n', result)
        self.assertNotIn('<br>', result)


if __name__ == '__main__':
    unittest.main()
