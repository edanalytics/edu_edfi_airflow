from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from typing import Dict, Any
from jinja2 import Template

class DynamicPythonOperator(BaseOperator):
    """
    Executes a Python callable with extended functionality to combine op_kwargs with static_kwargs.
    """

    template_fields = ('templates_dict', 'op_kwargs', 'static_kwargs', 'ds')
    template_ext = tuple()
    ui_color = '#ffefeb'

    @apply_defaults
    def __init__(
            self,
            python_callable,
            op_args=None,
            op_kwargs=None,
            provide_context=False,
            templates_dict=None,
            templates_exts=None,
            static_kwargs: Dict[str, Any] = None,
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.python_callable = python_callable
        self.op_args = op_args or []
        self.op_kwargs = op_kwargs or {}
        self.provide_context = provide_context
        self.templates_dict = templates_dict
        self.static_kwargs = static_kwargs or {}

        if templates_exts:
            self.template_ext = templates_exts

    def execute(self, context):
        """
        Execute the python function with the provided context.
        """
        # Combine op_kwargs and static_kwargs
        combined_kwargs = {**self.op_kwargs, **self.static_kwargs}

        if self.provide_context:
            # Update the context with combined kwargs
            context.update(combined_kwargs)
            context['templates_dict'] = self.templates_dict

            # Render Jinja templates for each context variable
            for key, value in context.items():
                if isinstance(value, str):
                    context[key] = self.render_jinja_template(value, context)

            # Filter out unnecessary context variables
            combined_kwargs = self._filter_kwargs(context)

        # Ensure combined_kwargs is a dictionary of the expected parameters
        return_value = self.execute_callable(**combined_kwargs)
        self.log.info("Done. Returned value was: %s", return_value)
        return return_value

    def execute_callable(self, **combined_kwargs):
        """
        Execute the python callable with combined_kwargs.
        """
        return self.python_callable(*self.op_args, **combined_kwargs)

    def _filter_kwargs(self, kwargs):
        """
        Filter out unnecessary context variables.
        """
        callable_args = self.python_callable.__code__.co_varnames
        return {k: v for k, v in kwargs.items() if k in callable_args}

    def render_jinja_template(self, template_string, context):
        """
        Render a Jinja template string with the given context.
        """
        template = Template(template_string)
        return template.render(**context)
