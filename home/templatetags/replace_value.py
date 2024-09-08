from django import template

register = template.Library()


@register.filter(name="replace_value")
def replace_value(value, args):
  return value.replace(args, " ").title()