from django.urls import path
from home import views
from django.contrib.auth import views as auth_views

urlpatterns = [
  # Dashboard
  path('', views.default, name='index'),
  path('dashboard/ecommerce/', views.dashboard_ecommerce, name='dashboard_ecommerce'),
  path('dashboard/crm/', views.dashboard_crm, name='dashboard_crm'),
  path('dashboard/analytics/', views.dashboard_analytics, name='dashboard_analytics'),
  path('dashboard/crypto/', views.dashboard_crypto, name='dashboard_crypto'),
  path('dashboard/project/', views.dashboard_project, name='dashboard_project'),

  # Page Layouts
  path('page-layouts/vertical/static-layout/', views.static_layout, name='static_layout'),
  path('page-layouts/vertical/fixed-layout/', views.fixed_layout, name='fixed_layout'),
  path('page-layouts/vertical/navbar-fixed-layout/', views.navbar_fixed_layout, name='navbar_fixed_layout'),
  path('page-layouts/vertical/collapse-layout/', views.collapse_layout, name='collapse_layout'),

  path('page-layouts/horizontal-layout/', views.horizontal_layout, name='horizontal_layout'),
  path('page-layouts/box-layout/', views.box_layout, name='box_layout'),
  path('page-layouts/rtl-layout/', views.rtl_layout, name='rtl_layout'),
  path('page-layouts/light-layout/', views.light_layout, name='light_layout'),
  path('page-layouts/dark-layout/', views.dark_layout, name='dark_layout'),
  path('page-layouts/color-icon-layout/', views.color_icon_layout, name='color_icon_layout'),

  # Widgets
  path('widget/statistic/', views.widget_statistic, name='widget_statistic'),
  path('widget/data/', views.widget_data, name='widget_data'),
  path('widget/table/', views.widget_table, name='widget_table'),
  path('widget/user-card/', views.widget_user_card, name='widget_user_card'),
  path('widget/chart/', views.widget_chart, name='widget_chart'),

  # User
  path('user/profile/', views.user_profile, name='user_profile'),
  path('user/social-profile/', views.social_profile, name='social_profile'),
  path('user/user-card/', views.user_card, name='user_card'),
  path('user/user-list/', views.user_list, name='user_list'),

  # Basic Components
  path('elements/basic/alert/', views.bc_alert, name='bc_alert'),
  path('elements/basic/button/', views.bc_button, name='bc_button'),
  path('elements/basic/badges/', views.bc_badges, name='bc_badges'),
  path('elements/basic/breadcrumb-pagination/', views.bc_breadcrumb_pagination, name='bc_breadcrumb_pagination'),
  path('elements/basic/cards/', views.bc_card, name='bc_card'),
  path('elements/basic/collapse/', views.bc_collapse, name='bc_collapse'),
  path('elements/basic/carousel/', views.bc_carousel, name='bc_carousel'),
  path('elements/basic/grid-system/', views.bc_grid, name='bc_grid'),
  path('elements/basic/progress/', views.bc_progress, name='bc_progress'),
  path('elements/basic/modal/', views.bc_modal, name='bc_modal'),
  path('elements/basic/spinner/', views.bc_spinner, name='bc_spinner'),
  path('elements/basic/navs-tabs/', views.bc_navs_tabs, name='bc_navs_tabs'),
  path('elements/basic/typography/', views.bc_typography, name='bc_typography'),
  path('elements/basic/tooltips-popover/', views.bc_tooltips_popover, name='bc_tooltips_popover'),
  path('elements/basic/toasts/', views.bc_toast, name='bc_toast'),
  path('elements/basic/extra/', views.bc_extra, name='bc_extra'),

  # Advance Components
  path('elements/advance/alert/', views.ac_alert, name='ac_alert'),
  path('elements/advance/datepicker/', views.ac_datepicker, name='ac_datepicker'),
  path('elements/advance/lightbox/', views.ac_lightbox, name='ac_lightbox'),
  path('elements/advance/modal/', views.ac_modal, name='ac_modal'),
  path('elements/advance/notification/', views.ac_notification, name='ac_notification'),
  path('elements/advance/pnotify/', views.ac_pnotify, name='ac_pnotify'),
  path('elements/advance/range-slider/', views.ac_rangeslider, name='ac_rangeslider'),
  path('elements/advance/slider/', views.ac_slider, name='ac_slider'),
  path('elements/advance/syntax-highlighter/', views.ac_syntax_highlighter, name='ac_syntax_highlighter'),
  path('elements/advance/tour/', views.ac_tour, name='ac_tour'),
  path('elements/advance/treeview/', views.ac_treeview, name='ac_treeview'),

  # Animation
  path('elements/animation/', views.animation, name='animation'),

  # Icons
  path('icons/feather-icon/', views.feather_icon, name='feather_icon'),
  path('icons/font-awesome/', views.font_awesome, name='font_awesome'),
  path('icons/flag-icon/', views.flag_icon, name='flag_icon'),
  path('icons/material-icon/', views.material_icon, name='material_icon'),
  path('icons/simple-line/', views.simple_line, name='simple_line'),
  path('icons/themify-icon/', views.themify_icon, name='themify_icon'),

  # Forms Layouts
  path('forms/form-basic/', views.form_basic, name='form_basic'),
  path('forms/form-elements/', views.form_elements, name='form_elements'),
  path('forms/input-group/', views.input_group, name='input_group'),
  path('forms/checkbox/', views.form_checkbox, name='form_checkbox'),
  path('forms/radio/', views.form_radio, name='form_radio'),
  path('forms/switch/', views.form_switch, name='form_switch'),
  path('forms/mega-option/', views.form_megaoption, name='form_megaoption'),

  # Form Plugins
  path('forms/datepicker/', views.form_datepicker, name="form_datepicker"),
  path('forms/date-range-picker/', views.form_date_rangepicker, name="form_date_rangepicker"),
  path('forms/timepicker/', views.form_timepicker, name="form_timepicker"),
  path('forms/choices/', views.form_choices, name="form_choices"),
  path('forms/google-recaptcha/', views.google_recaptcha, name="google_recaptcha"),
  path('forms/input-mask/', views.form_input_mask, name="form_input_mask"),
  path('forms/clipboard/', views.clipboard, name="clipboard"),
  path('forms/nouislider/', views.form_nouislider, name="form_nouislider"),
  path('forms/bootstrap-switch/', views.bs_form_switch, name="bs_form_switch"),
  path('forms/typeahead/', views.typeahead, name="typeahead"),

  # Text Editors
  path('forms/tinymce/', views.tinymce, name="tinymce"),
  path('forms/quill/', views.quill, name="quill"),
  path('forms/classic-editor/', views.classic_editor, name="classic_editor"),
  path('forms/document-editor/', views.document_editor, name="document_editor"),
  path('forms/inline-editor/', views.inline_editor, name="inline_editor"),
  path('forms/balloon-editor/', views.balloon_editor, name="balloon_editor"),
  path('forms/bootstrap-markdown/', views.bs_markdown, name="bs_markdown"),

  # Form Layouts
  path('forms/default-layout/', views.default_layout, name="default_layout"),
  path('forms/multicolumn-layout/', views.multicolumn_layout, name="multicolumn_layout"),
  path('forms/actionbars-layout/', views.actionbars_layout, name="actionbars_layout"),
  path('forms/sticky-actionbars-layout/', views.sticky_actionbars_layout, name="sticky_actionbars_layout"),

  # File Upload
  path('forms/dropzone/', views.dropzone, name="dropzone"),
  path('forms/uppy/', views.uppy, name="uppy"),

  path('form/validation/', views.form_validation, name="form_validation"),
  path('form/image-cropper/', views.image_cropper, name="image_cropper"),

  # Table
  path('table/bs-table/', views.bs_table, name="bs_table"),

  # Vanilla Table
  path('tables/basic-table/', views.basic_table, name="basic_table"),
  path('tables/dynamic-import/', views.dynamic_import, name="dynamic_import"),
  path('tables/render-column-cells/', views.render_column_cells, name="render_column_cells"),
  path('tables/column-manipulation/', views.column_manipulation, name="column_manipulation"),
  path('tables/datetime-sorting/', views.datetime_sorting, name="datetime_sorting"),
  path('tables/methods/', views.methods, name="methods"),
  path('tables/add-rows/', views.add_rows, name="add_rows"),
  path('tables/fetch-api/', views.fetch_api, name="fetch_api"),
  path('tables/filters/', views.filters, name="filters"),
  path('tables/export/', views.export, name="export"),

  # Data Table
  path('data-tables/basic-table/', views.dt_basic_table, name="dt_basic_table"),
  path('data-tables/advance-table/', views.dt_advance_table, name="dt_advance_table"),
  path('data-tables/styling-table/', views.dt_styling, name="dt_styling"),
  path('data-tables/api-initialization/', views.dt_api, name="dt_api"),
  path('data-tables/plugin-initialization/', views.dt_plugin, name="dt_plugin"),
  path('data-tables/data-sources/', views.dt_data_source, name="dt_data_source"),
  path('data-tables/data-sources/', views.dt_data_source, name="dt_data_source"),

  # DT Extensions
  path('data-tables/autofill/', views.dt_autofill, name="dt_autofill"),
  path('data-tables/basic-button/', views.dt_basic_button, name="dt_basic_button"),
  path('data-tables/data-export/', views.dt_data_export, name="dt_data_export"),
  path('data-tables/column-reorder/', views.dt_col_reorder, name="dt_col_reorder"),
  path('data-tables/fixed-column/', views.dt_fixed_columns, name="dt_fixed_columns"),
  path('data-tables/fixed-header/', views.dt_fixed_header, name="dt_fixed_header"),
  path('data-tables/key-table/', views.dt_key_table, name="dt_key_table"),
  path('data-tables/responsive/', views.dt_responsive, name="dt_responsive"),
  path('data-tables/row-reorder/', views.dt_row_reorder, name="dt_row_reorder"),
  path('data-tables/scroller/', views.dt_scroller, name="dt_scroller"),
  path('data-tables/select-table/', views.dt_select_table, name="dt_select_table"),

  # Charts
  path('charts/amchart-4/', views.amchart_4, name="amchart_4"),
  path('charts/chartjs/', views.chartjs, name="chartjs"),
  path('charts/google-chart/', views.chart_google, name="chart_google"),
  path('charts/highchart/', views.chart_highchart, name="chart_highchart"),
  path('charts/radial/', views.chart_radial, name="chart_radial"),

  # Maps
  path('maps/google-maps/', views.google_maps, name="google_maps"),
  path('maps/vector-maps/', views.vector_maps, name="vector_maps"),
  path('maps/map-api/', views.map_api, name="map_api"),
  path('maps/location-maps/', views.location_maps, name="location_maps"),

  # Landing Page
  path('landing-page/', views.landing_page, name="landing_page"),

  # Authentication -> Register
  path('accounts/register-v1/', views.RegistrationViewV1.as_view(), name="register_v1"),
  path('accounts/register-v2/', views.RegistrationViewV2.as_view(), name="register_v2"),
  path('accounts/register-v3/', views.RegistrationViewV3.as_view(), name="register_v3"),
  path('accounts/register-v4/', views.RegistrationViewV4.as_view(), name="register_v4"),
  path('accounts/register-v5/', views.RegistrationViewV5.as_view(), name="register_v5"),
  # Authentication -> Login
  path('accounts/login-v1/', views.LoginViewV1.as_view(), name="login_v1"),
  path('accounts/login-v2/', views.LoginViewV2.as_view(), name="login_v2"),
  path('accounts/login-v3/', views.LoginViewV3.as_view(), name="login_v3"),
  path('accounts/login-v4/', views.LoginViewV4.as_view(), name="login_v4"),
  path('accounts/login-v5/', views.LoginViewV5.as_view(), name="login_v5"),
  # Authentication -> Reset Password
  path('accounts/password-reset-v1/', views.PasswordResetV1.as_view(), name="password_reset_v1"),
  path('accounts/password-reset-v2/', views.PasswordResetV2.as_view(), name="password_reset_v2"),
  path('accounts/password-reset-v3/', views.PasswordResetV3.as_view(), name="password_reset_v3"),
  path('accounts/password-reset-v4/', views.PasswordResetV4.as_view(), name="password_reset_v4"),
  path('accounts/password-reset-v5/', views.PasswordResetV5.as_view(), name="password_reset_v5"),

  path('accounts/logout/', views.logout_view, name='logout'),

  path('accounts/change-password/', views.UserChangePasswordView.as_view(), name="change_password"),
  path('accounts/password-change-done/', auth_views.PasswordChangeDoneView.as_view(
    template_name="accounts/password-change-done.html"
  ), name="password_change_done"),

  path('accounts/password-reset-done/', auth_views.PasswordResetDoneView.as_view(
    template_name='accounts/password-reset-done.html'
  ), name='password_reset_done'),
  path('accounts/password-reset-confirm/<uidb64>/<token>/', 
    views.UserPasswordResetConfirmView.as_view(), name='password_reset_confirm'),
  path('accounts/password-reset-complete/', auth_views.PasswordResetCompleteView.as_view(
    template_name='accounts/password-reset-complete.html'
  ), name='password_reset_complete'),

  # Authentication -> Profile
  path('accounts/personal-information/', views.personal_information, name="personal_information"),
  path('accounts/profile-settings/', views.profile_settings, name="profile_settings"),
  path('accounts/map-form/', views.map_form, name="map_form"),
  path('accounts/subscribe-form/', views.subscribe_form, name="subscribe_form"),

  # Maintenance
  path('maint/error/', views.error, name="error"),
  path('maint/coming-soon/', views.coming_soon, name="coming_soon"),
  path('maint/offile/', views.offline_ui, name="offline"),

  # Social
  path('social/message/', views.messages, name="messages"),

  # Todo
  path('todo/', views.todo, name="todo"),

  # Gallery
  path('gallery/grid/', views.gallery_grid, name="gallery_grid"),
  path('gallery/masonry', views.gallery_masonry, name="gallery_masonry"),
  path('gallery/advance', views.gallery_advance, name="gallery_advance"),

  # Extension
  path('extension/invoice/', views.invoice, name="invoice"),
  path('extension/invoice-summary/', views.invoice_summary, name="invoice_summary"),
  path('extension/invoice-list/', views.invoice_list, name="invoice_list"),
  path('extension/full-calendar/', views.full_calendar, name="full_calendar"),

  path('extra/sample-page/', views.sample_page, name="sample_page"),

  # Index layouts
  path('index-1/', views.index_1, name="index_1"),
  path('index-2/', views.index_2, name="index_2"),
  path('index-2-2/', views.index_2_2, name="index_2_2"),
  path('index-3/', views.index_3, name="index_3"),
  path('index-4/', views.index_4, name="index_4"),
  path('index-4-2/', views.index_4_2, name="index_4_2"),
  path('index-5-h/', views.index_5h, name="index_5h"),
  path('index-light/', views.index_light, name="index_light"),
  path('index-6/', views.index_6, name="index_6"),
  path('index-8/', views.index_8, name="index_8"),
]
