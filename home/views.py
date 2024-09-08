from django.shortcuts import render, redirect
from django.contrib.auth.views import LoginView, PasswordResetView, PasswordChangeView, PasswordResetConfirmView
from home.forms import RegistrationForm, LoginForm, UserPasswordResetForm, UserSetPasswordForm, UserPasswordChangeForm
from django.contrib.auth import logout
from django.views.generic import CreateView

from django.contrib.auth.decorators import login_required


# Dashboard
def default(request):
  context = {
    'segment': 'default'
  }
  return render(request, 'pages/navigation/index.html', context)

@login_required(login_url='/accounts/login-v1/')
def dashboard_ecommerce(request):
  context = {
    'parent': 'dashboard',
    'segment': 'ecommerce'
  }
  return render(request, 'pages/navigation/dashboard-ecommerce.html', context)

@login_required(login_url='/accounts/login-v1/')
def dashboard_crm(request):
  context = {
    'parent': 'dashboard',
    'segment': 'crm'
  }
  return render(request, 'pages/navigation/dashboard-crm.html', context)

@login_required(login_url='/accounts/login-v1/')
def dashboard_analytics(request):
  context = {
    'parent': 'dashboard',
    'segment': 'analytics'
  }
  return render(request, 'pages/navigation/dashboard-analytics.html', context)

@login_required(login_url='/accounts/login-v1/')
def dashboard_crypto(request):
  context = {
    'parent': 'dashboard',
    'segment': 'crypto'
  }
  return render(request, 'pages/navigation/dashboard-crypto.html', context)

@login_required(login_url='/accounts/login-v1/')
def dashboard_project(request):
  context = {
    'parent': 'dashboard',
    'segment': 'project'
  }
  return render(request, 'pages/navigation/dashboard-project.html', context)

# Page Layouts

@login_required(login_url='/accounts/login-v1/')
def static_layout(request):
  context = {
    'parent': 'page_layouts',
    'segment': 'static_layouts'
  }
  return render(request, 'pages/navigation/layout-static.html', context)

@login_required(login_url='/accounts/login-v1/')
def fixed_layout(request):
  context = {
    'parent': 'page_layouts',
    'segment': 'fixed_layouts'
  }
  return render(request, 'pages/navigation/layout-fixed.html', context)

@login_required(login_url='/accounts/login-v1/')
def navbar_fixed_layout(request):
  context = {
    'parent': 'page_layouts',
    'segment': 'navbar-fixed_layouts'
  }
  return render(request, 'pages/navigation/layout-menu-fixed.html', context)

@login_required(login_url='/accounts/login-v1/')
def collapse_layout(request):
  context = {
    'parent': 'page_layouts',
    'segment': 'collapse_menu'
  }
  return render(request, 'pages/navigation/layout-mini-menu.html', context)

@login_required(login_url='/accounts/login-v1/')
def horizontal_layout(request):
  context = {
    'parent': 'page_layouts',
    'segment': 'horizontal_layout',
  }
  return render(request, 'pages/navigation/layout-horizontal.html', context)

@login_required(login_url='/accounts/login-v1/')
def box_layout(request):
  context = {
    'parent': 'page_layouts',
    'segment': 'box_layout'
  }
  return render(request, 'pages/navigation/layout-box.html', context)

@login_required(login_url='/accounts/login-v1/')
def rtl_layout(request):
  context = {
    'parent': 'page_layouts',
    'segment': 'rtl_layout'
  }
  return render(request, 'pages/navigation/layout-rtl.html', context)

@login_required(login_url='/accounts/login-v1/')
def light_layout(request):
  context = {
    'parent': 'page_layouts',
    'segment': 'light_layout'
  }
  return render(request, 'pages/navigation/layout-light.html', context)

@login_required(login_url='/accounts/login-v1/')
def dark_layout(request):
  context = {
    'parent': 'page_layouts',
    'segment': 'dark_layout'
  }
  return render(request, 'pages/navigation/layout-dark.html', context)

@login_required(login_url='/accounts/login-v1/')
def color_icon_layout(request):
  context = {
    'parent': 'page_layouts',
    'segment': 'color_icon_layout'
  }
  return render(request, 'pages/navigation/layout-menu-icon.html', context)

# Widgets
@login_required(login_url='/accounts/login-v1/')
def widget_statistic(request):
  context = {
    'parent': 'widget',
    'segment': 'widget_statistic'
  }
  return render(request, 'pages/navigation/widget-statistic.html', context)

@login_required(login_url='/accounts/login-v1/')
def widget_data(request):
  context = {
    'parent': 'widget',
    'segment': 'widget_data'
  }
  return render(request, 'pages/navigation/widget-data.html', context)

@login_required(login_url='/accounts/login-v1/')
def widget_table(request):
  context = {
    'parent': 'widget',
    'segment': 'widget_table'
  }
  return render(request, 'pages/navigation/widget-table.html', context)

@login_required(login_url='/accounts/login-v1/')
def widget_user_card(request):
  context = {
    'parent': 'widget',
    'segment': 'user_card'
  }
  return render(request, 'pages/navigation/widget-user-card.html', context)

@login_required(login_url='/accounts/login-v1/')
def widget_chart(request):
  context = {
    'parent': 'widget',
    'segment': 'widget_chart'
  }
  return render(request, 'pages/navigation/widget-chart.html', context)

# User
@login_required(login_url='/accounts/login-v1/')
def user_profile(request):
  context = {
    'parent': 'user',
    'segment': 'user_profile'
  }
  return render(request, 'pages/apps/user-profile.html', context)

@login_required(login_url='/accounts/login-v1/')
def social_profile(request):
  context = {
    'parent': 'user',
    'segment': 'social_profile'
  }
  return render(request, 'pages/navigation/user-profile-social.html', context)

@login_required(login_url='/accounts/login-v1/')
def user_card(request):
  context = {
    'parent': 'user',
    'segment': 'user_card'
  }
  return render(request, 'pages/navigation/user-card.html', context)

@login_required(login_url='/accounts/login-v1/')
def user_list(request):
  context = {
    'parent': 'user',
    'segment': 'user_list'
  }
  return render(request, 'pages/navigation/user-list.html', context)


# Basic Components
@login_required(login_url='/accounts/login-v1/')
def bc_alert(request):
  context = {
    'parent': 'basic_components',
    'segment': 'alert'
  }
  return render(request, 'pages/element/bc_alert.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_button(request):
  context = {
    'parent': 'basic_components',
    'segment': 'button'
  }
  return render(request, 'pages/element/bc_button.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_badges(request):
  context = {
    'parent': 'basic_components',
    'segment': 'badges'
  }
  return render(request, 'pages/element/bc_badges.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_breadcrumb_pagination(request):
  context = {
    'parent': 'basic_components',
    'segment': 'breadcrumbs_&_pagination'
  }
  return render(request, 'pages/element/bc_breadcrumb-pagination.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_card(request):
  context = {
    'parent': 'basic_components',
    'segment': 'cards'
  }
  return render(request, 'pages/element/bc_card.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_collapse(request):
  context = {
    'parent': 'basic_components',
    'segment': 'collapse'
  }
  return render(request, 'pages/element/bc_collapse.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_carousel(request):
  context = {
    'parent': 'basic_components',
    'segment': 'carousel'
  }
  return render(request, 'pages/element/bc_carousel.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_grid(request):
  context = {
    'parent': 'basic_components',
    'segment': 'grid'
  }
  return render(request, 'pages/element/bc_grid.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_progress(request):
  context = {
    'parent': 'basic_components',
    'segment': 'progress'
  }
  return render(request, 'pages/element/bc_progress.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_modal(request):
  context = {
    'parent': 'basic_components',
    'segment': 'modal'
  }
  return render(request, 'pages/element/bc_modal.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_spinner(request):
  context = {
    'parent': 'basic_components',
    'segment': 'spinner'
  }
  return render(request, 'pages/element/bc_spinner.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_navs_tabs(request):
  context = {
    'parent': 'basic_components',
    'segment': 'navs_&_tabs'
  }
  return render(request, 'pages/element/bc_tabs.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_typography(request):
  context = {
    'parent': 'basic_components',
    'segment': 'typography'
  }
  return render(request, 'pages/element/bc_typography.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_tooltips_popover(request):
  context = {
    'parent': 'basic_components',
    'segment': 'tooltip_&_popover'
  }
  return render(request, 'pages/element/bc_tooltip-popover.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_toast(request):
  context = {
    'parent': 'basic_components',
    'segment': 'toasts'
  }
  return render(request, 'pages/element/bc_toasts.html', context)

@login_required(login_url='/accounts/login-v1/')
def bc_extra(request):
  context = {
    'parent': 'basic_components',
    'segment': 'extra'
  }
  return render(request, 'pages/element/bc_extra.html', context)

# Advance Component
@login_required(login_url='/accounts/login-v1/')
def ac_alert(request):
  context = {
    'parent': 'advance_components',
    'segment': 'alert'
  }
  return render(request, 'pages/element/ac_alert.html', context)

@login_required(login_url='/accounts/login-v1/')
def ac_datepicker(request):
  context = {
    'parent': 'advance_components',
    'segment': 'datepicker'
  }
  return render(request, 'pages/element/ac_datepicker.html', context)

@login_required(login_url='/accounts/login-v1/')
def ac_lightbox(request):
  context = {
    'parent': 'advance_components',
    'segment': 'light_box'
  }
  return render(request, 'pages/element/ac_lightbox.html', context)

@login_required(login_url='/accounts/login-v1/')
def ac_modal(request):
  context = {
    'parent': 'advance_components',
    'segment': 'modal'
  }
  return render(request, 'pages/element/ac_modal.html', context)

@login_required(login_url='/accounts/login-v1/')
def ac_notification(request):
  context = {
    'parent': 'advance_components',
    'segment': 'notification'
  }
  return render(request, 'pages/element/ac_notification.html', context)

@login_required(login_url='/accounts/login-v1/')
def ac_pnotify(request):
  context = {
    'parent': 'advance_components',
    'segment': 'pnotify'
  }
  return render(request, 'pages/element/ac_pnotify.html', context)

@login_required(login_url='/accounts/login-v1/')
def ac_rangeslider(request):
  context = {
    'parent': 'advance_components',
    'segment': 'range_slider'
  }
  return render(request, 'pages/element/ac_rangeslider.html', context)

@login_required(login_url='/accounts/login-v1/')
def ac_slider(request):
  context = {
    'parent': 'advance_components',
    'segment': 'slider'
  }
  return render(request, 'pages/element/ac_slider.html', context)

@login_required(login_url='/accounts/login-v1/')
def ac_syntax_highlighter(request):
  context = {
    'parent': 'advance_components',
    'segment': 'syntax_highlighter'
  }
  return render(request, 'pages/element/ac_syntax_highlighter.html', context)

@login_required(login_url='/accounts/login-v1/')
def ac_tour(request):
  context = {
    'parent': 'advance_components',
    'segment': 'tour'
  }
  return render(request, 'pages/element/ac_tour.html', context)

@login_required(login_url='/accounts/login-v1/')
def ac_treeview(request):
  context = {
    'parent': 'advance_components',
    'segment': 'tree_view'
  }
  return render(request, 'pages/element/ac_treeview.html', context)

# Animation
@login_required(login_url='/accounts/login-v1/')
def animation(request):
  context = {
    'parent': 'animation',
  }
  return render(request, 'pages/element/animation.html', context)


# Icons
@login_required(login_url='/accounts/login-v1/')
def feather_icon(request):
  context = {
    'parent': 'icons',
    'segment': 'feather_icon'
  }
  return render(request, 'pages/element/icon-feather.html', context)

@login_required(login_url='/accounts/login-v1/')
def font_awesome(request):
  context = {
    'parent': 'icons',
    'segment': 'font_awesome_5'
  }
  return render(request, 'pages/element/icon-fontawesome.html', context)

@login_required(login_url='/accounts/login-v1/')
def flag_icon(request):
  context = {
    'parent': 'icons',
    'segment': 'flag'
  }
  return render(request, 'pages/element/icon-flag.html', context)

@login_required(login_url='/accounts/login-v1/')
def material_icon(request):
  context = {
    'parent': 'icons',
    'segment': 'material'
  }
  return render(request, 'pages/element/icon-material.html', context)

@login_required(login_url='/accounts/login-v1/')
def simple_line(request):
  context = {
    'parent': 'icons',
    'segment': 'simple_line'
  }
  return render(request, 'pages/element/icon-simple-line.html', context)

@login_required(login_url='/accounts/login-v1/')
def themify_icon(request):
  context = {
    'parent': 'icons',
    'segment': 'themify'
  }
  return render(request, 'pages/element/icon-themify.html', context)

# Forms Layouts
@login_required(login_url='/accounts/login-v1/')
def form_basic(request):
  context = {
    'parent': 'form_layouts',
    'segment': 'form_basic'
  }
  return render(request, 'pages/forms/form2_basic.html', context)

@login_required(login_url='/accounts/login-v1/')
def form_elements(request):
  context = {
    'parent': 'form_layouts',
    'segment': 'form_elements'
  }
  return render(request, 'pages/forms/form_elements.html', context)

@login_required(login_url='/accounts/login-v1/')
def input_group(request):
  context = {
    'parent': 'form_layouts',
    'segment': 'input_group'
  }
  return render(request, 'pages/forms/form2_input_group.html', context)

@login_required(login_url='/accounts/login-v1/')
def form_checkbox(request):
  context = {
    'parent': 'form_layouts',
    'segment': 'checkbox'
  }
  return render(request, 'pages/forms/form2_checkbox.html', context)

@login_required(login_url='/accounts/login-v1/')
def form_radio(request):
  context = {
    'parent': 'form_layouts',
    'segment': 'radio'
  }
  return render(request, 'pages/forms/form2_radio.html', context)

@login_required(login_url='/accounts/login-v1/')
def form_switch(request):
  context = {
    'parent': 'form_layouts',
    'segment': 'switch'
  }
  return render(request, 'pages/forms/form2_switch.html', context)

@login_required(login_url='/accounts/login-v1/')
def form_megaoption(request):
  context = {
    'parent': 'form_layouts',
    'segment': 'mega_option'
  }
  return render(request, 'pages/forms/form2_megaoption.html', context)

# Form Plugins
@login_required(login_url='/accounts/login-v1/')
def form_datepicker(request):
  context = {
    'parent': 'form_plugins',
    'segment': 'bootstrap_date_picker'
  }
  return render(request, 'pages/forms/form2_datepicker.html', context)

@login_required(login_url='/accounts/login-v1/')
def form_date_rangepicker(request):
  context = {
    'parent': 'form_plugins',
    'segment': 'date_range_picker'
  }
  return render(request, 'pages/forms/form2_daterangepicker.html', context)

@login_required(login_url='/accounts/login-v1/')
def form_timepicker(request):
  context = {
    'parent': 'form_plugins',
    'segment': 'timepicker'
  }
  return render(request, 'pages/forms/form2_timepicker.html', context)

@login_required(login_url='/accounts/login-v1/')
def form_choices(request):
  context = {
    'parent': 'form_plugins',
    'segment': 'choices_js'
  }
  return render(request, 'pages/forms/form2_choices.html', context)

@login_required(login_url='/accounts/login-v1/')
def google_recaptcha(request):
  context = {
    'parent': 'form_plugins',
    'segment': 'google_reCaptcha'
  }
  return render(request, 'pages/forms/form2_recaptcha.html', context)

@login_required(login_url='/accounts/login-v1/')
def form_input_mask(request):
  context = {
    'parent': 'form_plugins',
    'segment': 'form_masking'
  }
  return render(request, 'pages/forms/form2_inputmask.html', context)

@login_required(login_url='/accounts/login-v1/')
def clipboard(request):
  context = {
    'parent': 'form_plugins',
    'segment': 'clipboard'
  }
  return render(request, 'pages/forms/form2_clipboard.html', context)

@login_required(login_url='/accounts/login-v1/')
def form_nouislider(request):
  context = {
    'parent': 'form_plugins',
    'segment': 'noUiSlider'
  }
  return render(request, 'pages/forms/form2_nouislider.html', context)

@login_required(login_url='/accounts/login-v1/')
def bs_form_switch(request):
  context = {
    'parent': 'form_plugins',
    'segment': 'bootstrap_switch'
  }
  return render(request, 'pages/forms/form2_switchjs.html', context)

@login_required(login_url='/accounts/login-v1/')
def typeahead(request):
  context = {
    'parent': 'form_plugins',
    'segment': 'typeahead'
  }
  return render(request, 'pages/forms/form2_typeahead.html', context)

# Text Editors
@login_required(login_url='/accounts/login-v1/')
def tinymce(request):
  context = {
    'parent': 'form_text_editors',
    'segment': 'tinyMCE'
  }
  return render(request, 'pages/forms/form2_tinymce.html', context)

@login_required(login_url='/accounts/login-v1/')
def quill(request):
  context = {
    'parent': 'form_text_editors',
    'segment': 'quill_text_editor'
  }
  return render(request, 'pages/forms/form2_quill.html', context)

@login_required(login_url='/accounts/login-v1/')
def classic_editor(request):
  context = {
    'parent': 'editor',
    'segment': 'classic_editor'
  }
  return render(request, 'pages/forms/editor-classic.html', context)

@login_required(login_url='/accounts/login-v1/')
def document_editor(request):
  context = {
    'parent': 'editor',
    'segment': 'document_editor'
  }
  return render(request, 'pages/forms/editor-document.html', context)

@login_required(login_url='/accounts/login-v1/')
def inline_editor(request):
  context = {
    'parent': 'editor',
    'segment': 'inline_editor'
  }
  return render(request, 'pages/forms/editor-inline.html', context)

@login_required(login_url='/accounts/login-v1/')
def balloon_editor(request):
  context = {
    'parent': 'editor',
    'segment': 'balloon_editor'
  }
  return render(request, 'pages/forms/editor-balloon.html', context)

@login_required(login_url='/accounts/login-v1/')
def bs_markdown(request):
  context = {
    'parent': 'editor',
    'segment': 'bootstrap_markdown'
  }
  return render(request, 'pages/forms/form2_markdown.html', context)


# Form Layout
@login_required(login_url='/accounts/login-v1/')
def default_layout(request):
  context = {
    'parent': 'Form Layouts',
    'segment': 'default_forms'
  }
  return render(request, 'pages/forms/form2_lay-default.html', context)

@login_required(login_url='/accounts/login-v1/')
def multicolumn_layout(request):
  context = {
    'parent': 'Form Layouts',
    'segment': 'multi_column_forms'
  }
  return render(request, 'pages/forms/form2_lay-multicolumn.html', context)

@login_required(login_url='/accounts/login-v1/')
def actionbars_layout(request):
  context = {
    'parent': 'Form Layouts',
    'segment': 'basic_action_bars'
  }
  return render(request, 'pages/forms/form2_lay-actionbars.html', context)

@login_required(login_url='/accounts/login-v1/')
def sticky_actionbars_layout(request):
  context = {
    'parent': 'Form Layouts',
    'segment': 'sticky_action_bar'
  }
  return render(request, 'pages/forms/form2_lay-stickyactionbars.html', context)

# File Upload
@login_required(login_url='/accounts/login-v1/')
def dropzone(request):
  context = {
    'parent': 'file_upload',
    'segment': 'dropzone'
  }
  return render(request, 'pages/forms/file-upload.html', context)

@login_required(login_url='/accounts/login-v1/')
def uppy(request):
  context = {
    'parent': 'file_upload',
    'segment': 'uppy'
  }
  return render(request, 'pages/forms/form2_flu-uppy.html', context)

@login_required(login_url='/accounts/login-v1/')
def form_validation(request):
  context = {
    'parent': 'form_components',
    'segment': 'form_validation'
  }
  return render(request, 'pages/forms/form-validation.html', context)

@login_required(login_url='/accounts/login-v1/')
def image_cropper(request):
  context = {
    'parent': 'form_components',
    'segment': 'image_cropper'
  }
  return render(request, 'pages/forms/image_crop.html', context)

# Table
@login_required(login_url='/accounts/login-v1/')
def bs_table(request):
  context = {
    'parent': 'table',
    'segment': 'bootstrap_basic_tables'
  }
  return render(request, 'pages/table/tbl_bootstrap.html', context)

# Vanilla Table
@login_required(login_url='/accounts/login-v1/')
def basic_table(request):
  context = {
    'parent': 'table',
    'segment': 'basic_tables'
  }
  return render(request, 'pages/table/tbl_dt-simple.html', context)

@login_required(login_url='/accounts/login-v1/')
def dynamic_import(request):
  context = {
    'parent': 'table',
    'segment': 'dynamic_import'
  }
  return render(request, 'pages/table/tbl_dt-dynamic-import.html', context)

@login_required(login_url='/accounts/login-v1/')
def render_column_cells(request):
  context = {
    'parent': 'table',
    'segment': 'render_column_cells'
  }
  return render(request, 'pages/table/tbl_dt-render-column-cells.html', context)

@login_required(login_url='/accounts/login-v1/')
def column_manipulation(request):
  context = {
    'parent': 'table',
    'segment': 'column_manipulation'
  }
  return render(request, 'pages/table/tbl_dt-column-manipulation.html', context)

@login_required(login_url='/accounts/login-v1/')
def datetime_sorting(request):
  context = {
    'parent': 'table',
    'segment': 'datetime_sorting'
  }
  return render(request, 'pages/table/tbl_dt-datetime-sorting.html', context)

@login_required(login_url='/accounts/login-v1/')
def methods(request):
  context = {
    'parent': 'table',
    'segment': 'basic_tables'
  }
  return render(request, 'pages/table/tbl_dt-methods.html', context)

@login_required(login_url='/accounts/login-v1/')
def add_rows(request):
  context = {
    'parent': 'table',
    'segment': 'add_rows'
  }
  return render(request, 'pages/table/tbl_dt-add-rows.html', context)

@login_required(login_url='/accounts/login-v1/')
def fetch_api(request):
  context = {
    'parent': 'table',
    'segment': 'fetch_api'
  }
  return render(request, 'pages/table/tbl_dt-fetch-api.html', context)

@login_required(login_url='/accounts/login-v1/')
def filters(request):
  context = {
    'parent': 'table',
    'segment': 'filters'
  }
  return render(request, 'pages/table/tbl_dt-filters.html', context)

@login_required(login_url='/accounts/login-v1/')
def export(request):
  context = {
    'parent': 'table',
    'segment': 'export'
  }
  return render(request, 'pages/table/tbl_dt-export.html', context)

# Data Table
@login_required(login_url='/accounts/login-v1/')
def dt_basic_table(request):
  context = {
    'parent': 'data_table',
    'segment': 'basic_initialization'
  }
  return render(request, 'pages/table/dt_basic.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_advance_table(request):
  context = {
    'parent': 'data_table',
    'segment': 'advance_initialization'
  }
  return render(request, 'pages/table/dt_advance.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_styling(request):
  context = {
    'parent': 'data_table',
    'segment': 'styling_datatable'
  }
  return render(request, 'pages/table/dt_styling.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_api(request):
  context = {
    'parent': 'data_table',
    'segment': 'api_initialization'
  }
  return render(request, 'pages/table/dt_api.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_plugin(request):
  context = {
    'parent': 'data_table',
    'segment': 'plugin_initialization'
  }
  return render(request, 'pages/table/dt_plugin.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_data_source(request):
  context = {
    'parent': 'data_table',
    'segment': 'sources_initialization'
  }
  return render(request, 'pages/table/dt_sources.html', context)

# DT Extensions
@login_required(login_url='/accounts/login-v1/')
def dt_autofill(request):
  context = {
    'parent': 'data_table',
    'segment': 'autofill_initialization'
  }
  return render(request, 'pages/table/dt_ext_autofill.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_basic_button(request):
  context = {
    'parent': 'data_table',
    'segment': 'button_basic_initialization'
  }
  return render(request, 'pages/table/dt_ext_basic_buttons.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_basic_button(request):
  context = {
    'parent': 'data_table',
    'segment': 'button_basic_initialization'
  }
  return render(request, 'pages/table/dt_ext_basic_buttons.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_data_export(request):
  context = {
    'parent': 'data_table',
    'segment': 'html_5_data_datatable'
  }
  return render(request, 'pages/table/dt_ext_export_buttons.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_col_reorder(request):
  context = {
    'parent': 'data_table',
    'segment': 'columns_reorder'
  }
  return render(request, 'pages/table/dt_ext_col_reorder.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_fixed_columns(request):
  context = {
    'parent': 'data_table',
    'segment': 'fixed_columns'
  }
  return render(request, 'pages/table/dt_ext_fixed_columns.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_fixed_header(request):
  context = {
    'parent': 'data_table',
    'segment': 'fixed_header'
  }
  return render(request, 'pages/table/dt_ext_fixed_header.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_key_table(request):
  context = {
    'parent': 'data_table',
    'segment': 'key_table'
  }
  return render(request, 'pages/table/dt_ext_key_table.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_key_table(request):
  context = {
    'parent': 'data_table',
    'segment': 'key_table'
  }
  return render(request, 'pages/table/dt_ext_key_table.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_responsive(request):
  context = {
    'parent': 'data_table',
    'segment': 'responsive_datatable'
  }
  return render(request, 'pages/table/dt_ext_responsive.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_row_reorder(request):
  context = {
    'parent': 'data_table',
    'segment': 'rows_reorder'
  }
  return render(request, 'pages/table/dt_ext_row_reorder.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_scroller(request):
  context = {
    'parent': 'data_table',
    'segment': 'scroller'
  }
  return render(request, 'pages/table/dt_ext_scroller.html', context)

@login_required(login_url='/accounts/login-v1/')
def dt_select_table(request):
  context = {
    'parent': 'data_table',
    'segment': 'select_datatable'
  }
  return render(request, 'pages/table/dt_ext_select.html', context)

# Charts
@login_required(login_url='/accounts/login-v1/')
def amchart_4(request):
  context = {
    'parent': 'chart',
    'segment': 'amchart4'
  }
  return render(request, 'pages/chart-maps/chart-am.html', context)

@login_required(login_url='/accounts/login-v1/')
def chartjs(request):
  context = {
    'parent': 'chart',
    'segment': 'charts_js'
  }
  return render(request, 'pages/chart-maps/chart-chartjs.html', context)

@login_required(login_url='/accounts/login-v1/')
def chart_google(request):
  context = {
    'parent': 'chart',
    'segment': 'google_chart'
  }
  return render(request, 'pages/chart-maps/chart-google.html', context)

@login_required(login_url='/accounts/login-v1/')
def chart_highchart(request):
  context = {
    'parent': 'chart',
    'segment': 'highchart_chart'
  }
  return render(request, 'pages/chart-maps/chart-highchart.html', context)

@login_required(login_url='/accounts/login-v1/')
def chart_radial(request):
  context = {
    'parent': 'chart',
    'segment': 'radial_chart'
  }
  return render(request, 'pages/chart-maps/chart-radial.html', context)

# Maps
@login_required(login_url='/accounts/login-v1/')
def google_maps(request):
  context = {
    'parent': 'maps',
    'segment': 'google_maps'
  }
  return render(request, 'pages/chart-maps/map-google.html', context)

@login_required(login_url='/accounts/login-v1/')
def vector_maps(request):
  context = {
    'parent': 'maps',
    'segment': 'vector_map'
  }
  return render(request, 'pages/chart-maps/map-vector.html', context)

@login_required(login_url='/accounts/login-v1/')
def map_api(request):
  context = {
    'parent': 'maps',
    'segment': 'google_map_search_api'
  }
  return render(request, 'pages/chart-maps/map-api.html', context)

@login_required(login_url='/accounts/login-v1/')
def location_maps(request):
  context = {
    'parent': 'maps',
    'segment': 'location'
  }
  return render(request, 'pages/chart-maps/map-location.html', context)


# Landing Page

@login_required(login_url='/accounts/login-v1/')
def landing_page(request):
  return render(request, 'pages/landingpage.html')


# Authentication -> Register
class RegistrationViewV1(CreateView):
  template_name = 'account/signup.html'
  form_class = RegistrationForm
  success_url = '/accounts/login-v1/'

class RegistrationViewV2(CreateView):
  template_name = 'accounts/auth-signup-v2.html'
  form_class = RegistrationForm
  success_url = '/accounts/login-v2/'

class RegistrationViewV3(CreateView):
  template_name = 'accounts/auth-signup-v3.html'
  form_class = RegistrationForm
  success_url = '/accounts/login-v3/'

class RegistrationViewV4(CreateView):
  template_name = 'accounts/auth-signup-v4.html'
  form_class = RegistrationForm
  success_url = '/accounts/login-v4/'

class RegistrationViewV5(CreateView):
  template_name = 'accounts/auth-signup-v5.html'
  form_class = RegistrationForm
  success_url = '/accounts/login-v5/'


# Authentication -> Login
class LoginViewV1(LoginView):
  template_name = 'account/login.html'
  form_class = LoginForm

class LoginViewV2(LoginView):
  template_name = 'accounts/auth-signin-v2.html'
  form_class = LoginForm

class LoginViewV3(LoginView):
  template_name = 'accounts/auth-signin-v3.html'
  form_class = LoginForm

class LoginViewV4(LoginView):
  template_name = 'accounts/auth-signin-v4.html'
  form_class = LoginForm

class LoginViewV5(LoginView):
  template_name = 'accounts/auth-signin-v5.html'
  form_class = LoginForm


# Authentication -> Reset Password
class PasswordResetV1(PasswordResetView):
  template_name = 'accounts/auth-reset-password.html'
  form_class = UserPasswordResetForm

class PasswordResetV2(PasswordResetView):
  template_name = 'accounts/auth-reset-password-v2.html'
  form_class = UserPasswordResetForm

class PasswordResetV3(PasswordResetView):
  template_name = 'accounts/auth-reset-password-v3.html'
  form_class = UserPasswordResetForm

class PasswordResetV4(PasswordResetView):
  template_name = 'accounts/auth-reset-password-v4.html'
  form_class = UserPasswordResetForm

class PasswordResetV5(PasswordResetView):
  template_name = 'accounts/auth-reset-password-v5.html'
  form_class = UserPasswordResetForm

class UserPasswordResetConfirmView(PasswordResetConfirmView):
  template_name = 'accounts/password-reset-confirm.html'
  form_class = UserSetPasswordForm

# Authentication -> Change Password
class UserChangePasswordView(PasswordChangeView):
  template_name = 'accounts/auth-change-password.html'
  form_class = UserPasswordChangeForm

def logout_view(request):
  logout(request)
  return redirect('/accounts/login-v1/')


# Authentication -> Profile
@login_required(login_url='/accounts/login-v1/')
def personal_information(request):
  return render(request, 'pages/auth-personal-information.html')

@login_required(login_url='/accounts/login-v1/')
def profile_settings(request):
  return render(request, 'pages/auth-profile-settings.html')

@login_required(login_url='/accounts/login-v1/')
def map_form(request):
  return render(request, 'pages/auth-map-form.html')

@login_required(login_url='/accounts/login-v1/')
def subscribe_form(request):
  return render(request, 'pages/auth-subscribe.html')

# Maintenance
def error(request):
  return render(request, 'pages/maint-error.html')

def coming_soon(request):
  return render(request, 'pages/maint-coming-soon.html')

def offline_ui(request):
  return render(request, 'pages/maint-offline-ui.html')


# Social
@login_required(login_url='/accounts/login-v1/')
def messages(request):
  context = {
    'parent': 'social',
    'segment': 'message'
  }
  return render(request, 'pages/apps/message.html', context)

# Task
@login_required(login_url='/accounts/login-v1/')
def task_list(request):
  context = {
    'parent': 'task',
    'segment': 'task_list'
  }
  return render(request, 'pages/apps/task-list.html', context)

@login_required(login_url='/accounts/login-v1/')
def task_board(request):
  context = {
    'parent': 'task',
    'segment': 'task_board'
  }
  return render(request, 'pages/apps/task-board.html', context)

@login_required(login_url='/accounts/login-v1/')
def task_details(request):
  context = {
    'parent': 'task',
    'segment': 'task_detail'
  }
  return render(request, 'pages/apps/task-detail.html', context)


# Todo
@login_required(login_url='/accounts/login-v1/')
def todo(request):
  context = {
    'parent': 'to_do',
    'segment': 'to_do'
  }
  return render(request, 'pages/apps/todo.html', context)

# Gallery
@login_required(login_url='/accounts/login-v1/')
def gallery_grid(request):
  context = {
    'parent': 'gallery',
    'segment': 'gallery_grid'
  }
  return render(request, 'pages/apps/gallery-grid.html', context)

@login_required(login_url='/accounts/login-v1/')
def gallery_masonry(request):
  context = {
    'parent': 'gallery',
    'segment': 'gallery_masonry'
  }
  return render(request, 'pages/apps/gallery-masonry.html', context)

@login_required(login_url='/accounts/login-v1/')
def gallery_advance(request):
  context = {
    'parent': 'gallery',
    'segment': 'gallery_advance'
  }
  return render(request, 'pages/apps/gallery-advance.html', context)

# Extension
@login_required(login_url='/accounts/login-v1/')
def invoice(request):
  context = {
    'parent': 'extension',
    'segment': 'invoice'
  }
  return render(request, 'pages/extension/invoice.html', context)

@login_required(login_url='/accounts/login-v1/')
def invoice_summary(request):
  context = {
    'parent': 'extension',
    'segment': 'invoice_summary'
  }
  return render(request, 'pages/extension/invoice-summary.html', context)

@login_required(login_url='/accounts/login-v1/')
def invoice_list(request):
  context = {
    'parent': 'extension',
    'segment': 'invoice_list'
  }
  return render(request, 'pages/extension/invoice-list.html', context)

@login_required(login_url='/accounts/login-v1/')
def full_calendar(request):
  context = {
    'parent': 'extension',
    'segment': 'full_calendar'
  }
  return render(request, 'pages/extension/full-calendar.html', context)

@login_required(login_url='/accounts/login-v1/')
def sample_page(request):
  context = {
    'parent': 'extra',
    'segment': 'sample_page'
  }
  return render(request, 'pages/other/sample-page.html', context)


# Index layouts
def index_1(request):
  return render(request, 'pages/navigation/index-1.html')

def index_2(request):
  return render(request, 'pages/navigation/index-2.html')

def index_2_2(request):
  return render(request, 'pages/navigation/index-2-2.html')

def index_3(request):
  return render(request, 'pages/navigation/index-3.html')

def index_4(request):
  return render(request, 'pages/navigation/index-4.html')

def index_4_2(request):
  return render(request, 'pages/navigation/index-4-2.html')

def index_5h(request):
  return render(request, 'pages/navigation/index-5-h.html')

def index_light(request):
  return render(request, 'pages/navigation/index-light.html')

def index_6(request):
  context = {
    'parent': 'layout',
    'segment': 'index_6'
  }
  return render(request, 'pages/navigation/index-6.html', context)

def index_8(request):
  context = {
    'parent': 'layout',
    'segment': 'index_8'
  }
  return render(request, 'pages/navigation/index-8.html', context)

def handler404(request, exception=None):
  return render(request, 'accounts/error-404.html')

def handler403(request, exception=None):
  return render(request, 'accounts/error-403.html')

def handler500(request, exception=None):
  return render(request, 'accounts/error-500.html')

# i18n
def i18n_view(request):
  context = {
    'parent': 'apps',
    'segment': 'i18n'
  }
  return render(request, 'pages/navigation/i18n.html', context)