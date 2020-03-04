<%inherit file="../Enum.java" />
<%block name="custom_methods">
    public boolean isFeed() {
        switch (this) {
        % for note_name, _ in items:
            % if note_name.endswith('_FEED'):
            case ${note_name}:
            % endif
        % endfor
                return true;
        }
        return false;
    }
</%block>
