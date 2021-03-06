import React, { Component, PropTypes } from "react";
import cx from "classnames";

import FormField from "metabase/components/form/FormField.jsx";
import FormLabel from "metabase/components/form/FormLabel.jsx";
import FormMessage from "metabase/components/form/FormMessage.jsx";


// TODO - this should be somewhere more centralized
function isEmpty(str) {
    return (!str || 0 === str.length);
}

/**
 * This is a form for capturing database details for a given `engine` supplied via props.
 * The intention is to encapsulate the entire <form> with standard MB form styling and allow a callback
 * function to receive the captured form input when the form is submitted.
 */
export default class DatabaseDetailsForm extends Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            details: props.details || {},
            valid: false
        }
    }

    static propTypes = {
        details: PropTypes.object,
        engine: PropTypes.string.isRequired,
        engines: PropTypes.object.isRequired,
        formError: PropTypes.object,
        hiddenFields: PropTypes.object,
        submitButtonText: PropTypes.string.isRequired,
        submitFn: PropTypes.func.isRequired
    };

    validateForm() {
        let { engine, engines } = this.props;
        let { details } = this.state;

        let valid = true;

        // name is required
        if (!details.name) {
            valid = false;
        }

        // go over individual fields
        for (let field of engines[engine]['details-fields']) {
            if (field.required && isEmpty(details[field.name])) {
                valid = false;
                break;
            }
        }

        if (this.state.valid !== valid) {
            this.setState({ valid });
        }
    }

    componentDidMount() {
        this.validateForm();
    }

    componentDidUpdate() {
        this.validateForm();
    }

    onChange(fieldName, fieldValue) {
        this.setState({ details: { ...this.state.details, [fieldName]: fieldValue }});
    }

    formSubmitted(e) {
        e.preventDefault();

        let { engine, engines, submitFn } = this.props;
        let { details } = this.state;

        let request = {
            engine: engine,
            name: details.name,
            details: {}
        };

        for (let field of engines[engine]['details-fields']) {
            let val = details[field.name] === "" ? null : details[field.name];

            if (val && field.type === 'integer') val = parseInt(val);
            if (val == null && field.default)    val = field.default;

            request.details[field.name] = val;
        }

        submitFn(request);
    }

    renderFieldInput(field, fieldIndex) {
        let { details } = this.state;
        let value = details && details[field.name] || "";

        switch(field.type) {
            case 'boolean':
                return (
                    <div className="Form-input Form-offset full Button-group">
                        <div className={cx('Button', details[field.name] === true ? 'Button--active' : null)} onClick={(e) => { this.onChange(field.name, true) }}>
                            Yes
                        </div>
                        <div className={cx('Button', details[field.name] === false ? 'Button--danger' : null)} onClick={(e) => { this.onChange(field.name, false) }}>
                            No
                        </div>
                    </div>
                );
            default:
                return (
                    <input
                        type={field.type === 'password' ? 'password' : 'text'}
                        className="Form-input Form-offset full"
                        ref={field.name}
                        name={field.name}
                        value={value}
                        placeholder={field.default || field.placeholder}
                        onChange={(e) => this.onChange(field.name, e.target.value)}
                        required={field.required}
                        autoFocus={fieldIndex === 0}
                    />
                );
        }
    }

    render() {
        let { engine, engines, formError, formSuccess, hiddenFields, submitButtonText } = this.props;
        let { valid } = this.state;

        let fields = [
            {
                name: 'name',
                'display-name': 'Name',
                placeholder: "How would you like to refer to this database?",
                required: true
            },
            ...engines[engine]['details-fields']
        ];

        hiddenFields = hiddenFields || {};

        return (
            <form onSubmit={this.formSubmitted.bind(this)} noValidate>
                <div className="FormInputGroup">
                    { fields.filter(field => !hiddenFields[field.name]).map((field, fieldIndex) =>
                        <FormField key={field.name} fieldName={field.name}>
                            <FormLabel title={field['display-name']} fieldName={field.name}></FormLabel>
                            {this.renderFieldInput(field, fieldIndex)}
                            <span className="Form-charm"></span>
                        </FormField>
                      )}
                </div>

                <div className="Form-actions">
                    <button className={cx("Button", {"Button--primary": valid})} disabled={!valid}>
                        {submitButtonText}
                    </button>
                    <FormMessage formError={formError} formSuccess={formSuccess}></FormMessage>
                </div>
            </form>
        );
    }
}
